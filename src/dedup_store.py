import sqlite3
from typing import Set, Tuple
from pathlib import Path

# Nama file database SQLite yang akan digunakan.
DB_FILE = "dedup_store.sqlite"
# Ukuran cache in-memory untuk lookup cepat.
CACHE_SIZE = 100000

class DeduplicationStore:
    """
    Menyimpan pasangan (topic, event_id) yang telah diproses secara persisten
    menggunakan SQLite.
    """
    def __init__(self, db_path: Path):
        self.db_path = db_path
        # Cache in-memory untuk lookup cepat (mengurangi hit disk)
        self._processed_cache: Set[Tuple[str, str]] = set()
        self.conn = None
        self._initialize_db()

    def _initialize_db(self):
        """Membuat koneksi dan tabel jika belum ada, serta memuat cache."""
        print(f"[DEDUP] Menginisialisasi basis data di: {self.db_path}")
        # Gunakan check_same_thread=False karena FastAPI/uvicorn berjalan multi-thread
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        
        # Membuat tabel 'processed_events' dengan PRIMARY KEY gabungan
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                topic TEXT NOT NULL,
                event_id TEXT NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (topic, event_id)
            )
        """)
        self.conn.commit()
        self._load_cache()

    def _load_cache(self):
        """Memuat N entri terakhir ke dalam cache in-memory saat startup dari disk."""
        try:
            res = self.cursor.execute(f"""
                SELECT topic, event_id FROM processed_events
                ORDER BY processed_at DESC
                LIMIT {CACHE_SIZE}
            """)
            self._processed_cache = set(res.fetchall())
            print(f"[DEDUP] Memuat {len(self._processed_cache)} entri ke cache.")
        except Exception as e:
            print(f"[ERROR] Gagal memuat cache: {e}")

    def is_processed(self, topic: str, event_id: str) -> bool:
        """Memeriksa apakah event ini telah diproses (Cek Cache -> Cek Disk)."""
        key = (topic, event_id)
        
        # 1. Cek Cache In-memory
        if key in self._processed_cache:
            return True

        # 2. Cek Disk (SQLite)
        res = self.cursor.execute("""
            SELECT 1 FROM processed_events WHERE topic = ? AND event_id = ?
        """, (topic, event_id)).fetchone()
        
        if res is not None:
            self._processed_cache.add(key)
            return True
            
        return False

    def mark_processed(self, topic: str, event_id: str):
        """Menandai event sebagai telah diproses dan menyimpannya secara persisten."""
        key = (topic, event_id)
        self._processed_cache.add(key)
        
        try:
            # Memasukkan ke database.
            self.cursor.execute("""
                INSERT OR IGNORE INTO processed_events (topic, event_id) VALUES (?, ?)
            """, (topic, event_id))
            
            # Commit setiap kali untuk memastikan persistensi instan (Toleransi Crash)
            self.conn.commit()
        except Exception as e:
             print(f"[ERROR] Gagal menyimpan ke DB: {e}")


    def close(self):
        """Menutup koneksi database."""
        if self.conn:
            self.conn.close()

# Inisialisasi store secara global
DEDUP_STORE = DeduplicationStore(Path(DB_FILE))