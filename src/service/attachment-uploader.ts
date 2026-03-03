import { readFile, stat } from "node:fs/promises";
import { basename } from "node:path";
import type { Opik } from "opik";
import {
  ATTACHMENT_UPLOAD_PART_SIZE_BYTES,
  LOCAL_ATTACHMENT_UPLOAD_MAGIC_ID,
} from "./constants.js";
import { collectMediaPathsFromUnknown, guessMimeType, resolveEntityId } from "./media.js";

type AttachmentsApi = {
  startMultiPartUpload: (request: {
    fileName: string;
    numOfFileParts: number;
    mimeType?: string;
    projectName?: string;
    entityType: "trace" | "span";
    entityId: string;
    path: string;
  }) => Promise<{ uploadId: string; preSignUrls: string[] }>;
  completeMultiPartUpload: (request: {
    fileName: string;
    projectName?: string;
    entityType: "trace" | "span";
    entityId: string;
    fileSize: number;
    mimeType?: string;
    uploadId: string;
    uploadedFileParts: Array<{ eTag: string; partNumber: number }>;
  }) => Promise<void>;
};

type OpikWithAttachmentsApi = Opik & {
  api?: {
    attachments?: AttachmentsApi;
  };
};

type AttachmentUploaderDeps = {
  getClient: () => Opik | null;
  getAttachmentBaseUrl: () => string;
  onWarn: (message: string) => void;
  formatError: (err: unknown) => string;
};

export type ScheduledMediaUpload = {
  entityType: "trace" | "span";
  entity: unknown;
  projectName: string;
  reason: string;
  payloads: unknown[];
};

export function createAttachmentUploader(deps: AttachmentUploaderDeps) {
  let attachmentQueue: Promise<void> = Promise.resolve();
  const uploadedAttachmentKeys = new Set<string>();

  function scheduleAttachmentUpload(job: () => Promise<void>): void {
    attachmentQueue = attachmentQueue.then(job).catch((err: unknown) => {
      deps.onWarn(`opik: attachment upload task failed: ${deps.formatError(err)}`);
    });
  }

  async function uploadFileAttachment(params: {
    entityType: "trace" | "span";
    entityId: string;
    projectName: string;
    filePath: string;
    reason: string;
  }): Promise<void> {
    const baseClient = deps.getClient();
    if (!baseClient) return;

    const existingKey = `${params.entityType}:${params.entityId}:${params.filePath}`;
    if (uploadedAttachmentKeys.has(existingKey)) return;
    uploadedAttachmentKeys.add(existingKey);

    const client = baseClient as OpikWithAttachmentsApi;
    const attachmentsApi = client.api?.attachments;
    if (!attachmentsApi) return;

    try {
      const stats = await stat(params.filePath);
      if (!stats.isFile() || stats.size <= 0) return;

      const bytes = await readFile(params.filePath);
      const totalSize = bytes.byteLength;
      const mimeType = guessMimeType(params.filePath);
      const fileName = basename(params.filePath) || "attachment.bin";
      const partCount = Math.max(1, Math.ceil(totalSize / ATTACHMENT_UPLOAD_PART_SIZE_BYTES));
      const pathBase64 = Buffer.from(deps.getAttachmentBaseUrl(), "utf8").toString("base64");

      const started = await attachmentsApi.startMultiPartUpload({
        fileName,
        numOfFileParts: partCount,
        mimeType,
        projectName: params.projectName,
        entityType: params.entityType,
        entityId: params.entityId,
        path: pathBase64,
      });

      const urls = started.preSignUrls ?? [];
      if (urls.length === 0) return;

      if (started.uploadId === LOCAL_ATTACHMENT_UPLOAD_MAGIC_ID) {
        const localResponse = await fetch(urls[0], {
          method: "PUT",
          body: bytes,
        });
        if (!localResponse.ok) {
          throw new Error(`local attachment upload failed status=${localResponse.status}`);
        }
        return;
      }

      if (urls.length < partCount) {
        throw new Error(
          `insufficient pre-signed URLs (got ${urls.length}, expected ${partCount})`,
        );
      }

      const uploadedParts: Array<{ eTag: string; partNumber: number }> = [];
      for (let partNumber = 1; partNumber <= partCount; partNumber++) {
        const start = (partNumber - 1) * ATTACHMENT_UPLOAD_PART_SIZE_BYTES;
        const end = Math.min(start + ATTACHMENT_UPLOAD_PART_SIZE_BYTES, totalSize);
        const chunk = bytes.subarray(start, end);
        const url = urls[partNumber - 1];

        const partResponse = await fetch(url, {
          method: "PUT",
          body: chunk,
        });
        if (!partResponse.ok) {
          throw new Error(
            `attachment part upload failed status=${partResponse.status} part=${partNumber}/${partCount}`,
          );
        }

        const eTag = partResponse.headers.get("etag") ??
          partResponse.headers.get("ETag") ??
          "";
        uploadedParts.push({ eTag, partNumber });
      }

      await attachmentsApi.completeMultiPartUpload({
        fileName,
        projectName: params.projectName,
        entityType: params.entityType,
        entityId: params.entityId,
        fileSize: totalSize,
        mimeType,
        uploadId: started.uploadId,
        uploadedFileParts: uploadedParts,
      });
    } catch (err) {
      uploadedAttachmentKeys.delete(existingKey);
      deps.onWarn(
        `opik: attachment upload failed (${params.reason}, entity=${params.entityType}:${params.entityId}, path=${params.filePath}): ${deps.formatError(err)}`,
      );
    }
  }

  function scheduleMediaAttachmentUploads(params: ScheduledMediaUpload): void {
    const entityId = resolveEntityId(params.entity);
    if (!entityId) return;

    const mediaPaths = new Set<string>();
    for (const payload of params.payloads) {
      collectMediaPathsFromUnknown(payload, mediaPaths);
    }
    if (mediaPaths.size === 0) return;

    for (const filePath of mediaPaths) {
      scheduleAttachmentUpload(() =>
        uploadFileAttachment({
          entityType: params.entityType,
          entityId,
          projectName: params.projectName,
          filePath,
          reason: params.reason,
        })
      );
    }
  }

  async function waitForUploads(): Promise<void> {
    await attachmentQueue.catch(() => undefined);
  }

  function reset(): void {
    uploadedAttachmentKeys.clear();
  }

  return {
    scheduleMediaAttachmentUploads,
    waitForUploads,
    reset,
  };
}
