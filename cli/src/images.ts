/** How alasio names the images it runs. */
import { VERSION } from "./release.ts";

/** An image: its repository, its tag (this package's version when empty), and its digest (none when empty). */
export interface ImageRef {
  readonly repository: string;
  readonly tag: string;
  readonly digest: string;
}

/** `image`'s reference, pinned by its digest when it has one. */
export function imageReference(image: ImageRef): string {
  return `${image.repository}:${image.tag || VERSION}${image.digest ? `@${image.digest}` : ""}`;
}
