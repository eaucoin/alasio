import type { Audio, Document, Message, PhotoSize, Video, Voice } from "@grammyjs/types";

/** A file an incoming message carries, as Telegram describes it, with a kind and a name to save it under. */
export type IncomingFile = IncomingDocument | IncomingPhoto | IncomingVideo | IncomingAudio | IncomingVoice;

export interface IncomingDocument extends Document {
  kind: "document";
  file_name: string;
}

/** The largest size Telegram offers of a photo. */
export interface IncomingPhoto extends PhotoSize {
  kind: "photo";
  file_name: string;
  mime_type: "image/jpeg";
}

export interface IncomingVideo extends Video {
  kind: "video";
  file_name: string;
}

export interface IncomingAudio extends Audio {
  kind: "audio";
  file_name: string;
}

export interface IncomingVoice extends Voice {
  kind: "voice";
  file_name: string;
  mime_type: string;
}

export function getMessageText(message: Message): string {
  return String(message.text ?? message.caption ?? "").trim();
}

export function getMessageFiles(message: Message): IncomingFile[] {
  const files: IncomingFile[] = [];
  if (message.document?.file_id) {
    files.push({
      ...message.document,
      kind: "document",
      file_name: message.document.file_name ?? `document-${message.message_id}`,
    });
  }
  const largest = Array.isArray(message.photo)
    ? [...message.photo].sort((a, b) => (b.file_size ?? 0) - (a.file_size ?? 0))[0]
    : undefined;
  if (largest) {
    files.push({
      ...largest,
      kind: "photo",
      file_name: `photo-${message.message_id}.jpg`,
      mime_type: "image/jpeg",
    });
  }
  if (message.video?.file_id) {
    files.push({
      ...message.video,
      kind: "video",
      file_name: message.video.file_name ?? `video-${message.message_id}.mp4`,
    });
  }
  if (message.audio?.file_id) {
    files.push({
      ...message.audio,
      kind: "audio",
      file_name: message.audio.file_name ?? `audio-${message.message_id}`,
    });
  }
  if (message.voice?.file_id) {
    files.push({
      ...message.voice,
      kind: "voice",
      file_name: `voice-${message.message_id}.ogg`,
      mime_type: message.voice.mime_type ?? "audio/ogg",
    });
  }
  return files;
}
