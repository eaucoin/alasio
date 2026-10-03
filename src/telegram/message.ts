// @ts-nocheck
export function getMessageText(message) {
  return String(message.text ?? message.caption ?? "").trim();
}

export function getMessageFiles(message) {
  const files = [];
  if (message.document?.file_id) {
    files.push({
      ...message.document,
      kind: "document",
      file_name: message.document.file_name ?? `document-${message.message_id}`,
    });
  }
  if (Array.isArray(message.photo) && message.photo.length > 0) {
    const largest = [...message.photo].sort((a, b) => (b.file_size ?? 0) - (a.file_size ?? 0))[0];
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
