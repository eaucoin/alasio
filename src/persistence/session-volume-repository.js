/**
 * The durable record of each session filesystem: its metadata namespace (a Valkey DB
 * index, unique per volume) and whether its JuiceFS volume has been formatted yet. The
 * workspace a conversation points at is still carried in conversations.working_directory
 * (as the sentinel `sessionfs:<volumeId>`, see ../workspace/kind.js); this table holds
 * only what the volume manager needs to mount and destroy it.
 */
export class SqliteSessionVolumeRepository {
  constructor(db) {
    this.db = db;
  }

  /**
   * Reserve the lowest free namespace in `[first, first+count)` for a new volume and
   * record it, atomically. Throws if the volume already exists or no namespace is free.
   */
  reserveVolume(volumeId, first, count) {
    const reserve = this.db.transaction(() => {
      if (this.db.prepare("select 1 from session_volumes where id = ?").get(volumeId)) {
        throw new Error(`session volume already exists: ${volumeId}`);
      }
      const used = new Set(
        this.db.prepare("select db_index from session_volumes").all().map((row) => row.db_index),
      );
      let dbIndex = -1;
      for (let candidate = first; candidate < first + count; candidate += 1) {
        if (!used.has(candidate)) { dbIndex = candidate; break; }
      }
      if (dbIndex < 0) throw new Error("no free session-filesystem metadata namespace (raise the metadata databases limit)");
      this.db.prepare("insert into session_volumes (id, db_index, formatted) values (?, ?, 0)").run(volumeId, dbIndex);
      return { dbIndex };
    });
    return reserve();
  }

  getVolume(volumeId) {
    const row = this.db.prepare("select id, db_index, formatted from session_volumes where id = ?").get(volumeId);
    return row ? { volumeId: row.id, dbIndex: row.db_index, formatted: row.formatted === 1 } : null;
  }

  setVolumeFormatted(volumeId, formatted) {
    this.db.prepare("update session_volumes set formatted = ? where id = ?").run(formatted ? 1 : 0, volumeId);
  }

  deleteVolume(volumeId) {
    this.db.prepare("delete from session_volumes where id = ?").run(volumeId);
  }

  listVolumes() {
    return this.db
      .prepare("select id, db_index, formatted from session_volumes order by db_index")
      .all()
      .map((row) => ({ volumeId: row.id, dbIndex: row.db_index, formatted: row.formatted === 1 }));
  }
}
