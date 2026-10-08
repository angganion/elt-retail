# Transform — Medallion Layers (Bronze → Silver → Gold)

Folder ini berisi transformasi data dari **landing (data mentah di GCS) menjadi
tabel analitik siap pakai** di DuckDB. Semua transformasi ditulis sebagai SQL
dan dijalankan berurutan oleh `transform.py`:

```text
data/retail-datalake/landing/...  (Parquet mentah)
        │
        ▼
  bronze.sql   →  view "jendela" ke file landing        (data mentah, tidak diubah)
        │
        ▼
  silver.sql   →  tabel bersih + riwayat SCD Type 2     (dedupe, validasi, riwayat)
        │
        ▼
  gold.sql     →  tabel agregat siap analisis           (summary untuk dashboard)
```

Ini pola **medallion architecture** yang umum di data engineering: data mengalir
dari mentah → bersih → siap pakai, dan tiap layer bisa ditelusuri balik sampai
ke sumber aslinya.

Jalankan dari root repo: `python3 transform/transform.py`

---

## Bronze — data mentah apa adanya

**File:** `bronze.sql`

Layer paling bawah. Membuat *view* (bukan tabel) untuk setiap sumber data di
landing: `orders`, `customers`, `products`, `categories`, `order_items`
(dari MySQL retailerdb), `suppliers`, `product_suppliers` (dari MySQL
supplierdb), dan `customer_reviews` (dari API).

**Kenapa view?** Karena bronze adalah "jendela" ke file landing — datanya
selalu dibaca langsung dari Parquet setiap kali dipanggil, tidak disalin ke
DuckDB. Ini padanan dari **external table** di BigQuery (yang menunjuk ke file
di GCS). Manfaatnya: transformasi tidak duplikasi data, dan kalau file landing
berubah, bronze otomatis ikut berubah.

**Tujuannya:** menyediakan sumber yang seragam untuk layer di atasnya. Semua
layer setelah bronze membaca dari sini — bukan langsung dari file — jadi kalau
struktur file berubah, cukup diubah di satu tempat.

---

## Silver — data bersih + riwayat perubahan

**File:** `silver.sql`

Layer tengah. Membersihkan dan menstandarkan data dari bronze:

- **Deduplikasi** — tiap sumber di-stage ke temp table dengan `SELECT DISTINCT`
  supaya data duplikat tidak ikut terproses.
- **Validasi / karantina** — baris dengan kolom wajib NULL ditandai
  `is_quarantined = TRUE` (misal `customer_id`/`email` NULL). Data kotor ini
  tetap disimpan, tapi tidak dipakai di layer gold.
- **Riwayat SCD Type 2** — tabel yang datanya bisa berubah (`customers`,
  `orders`, `product_suppliers`, `customer_reviews`) memakai
  **Slowly Changing Dimension Type 2**: kalau data berubah, baris lama tidak
  dihapus — ditandai `is_active = FALSE` + `effective_end_date` di tabel
  `*_history`, lalu baris baru di-insert dengan `is_active = TRUE`. Hasilnya
  riwayat lengkap "siapa dulu seperti apa", pola standar data warehouse
  untuk dimensi. Gold TIDAK membaca tabel riwayat ini, melainkan VIEW base
  (`silver.customers` dsb.) yang hanya berisi baris aktif — dan view itu
   sengaja tanpa kolom `is_active`, sehingga tidak ada filter versi yang dapat
   dilupakan.
- **Strategi per tabel** — tabel kecil yang jarang berubah (`categories`,
  `products`, `suppliers`) cukup **TRUNCATE + INSERT ulang** tiap run (lebih
  sederhana), sedangkan tabel besar yang perubahannya bermakna bisnis memakai
  **MERGE** (SCD Type 2) supaya riwayat tetap terjaga. `order_items` sengaja
  TRUNCATE + INSERT sejak 2026-09: ia fact transaksional, bukan dimensi —
  versinya tak punya pembaca, tiap join gold membayar pajak filter untuknya.

**Tujuannya:** menyediakan data yang bersih, valid, dan ber-riwayat untuk
layer agregat. Query di gold tidak perlu lagi memikirkan duplikasi, versi,
atau data NULL — semua sudah ditangani di sini.

> **Catatan tiap tabel silver:**
> - `customers` (view aktif-saja atas `customers_history`) — perubahan nama/email dicatat sebagai riwayat.
> - `orders` (view aktif-saja atas `orders_history`) — perubahan nilai order dicatat; gold otomatis hanya melihat versi terkini.
> - `order_items` (TRUNCATE + INSERT) — fact, tanpa riwayat; ada flag `is_quarantined`.
> - `product_suppliers` (view aktif-saja atas `*_history`) — relasi supplier–produk dengan primary key gabungan.
> - `customer_reviews` (view aktif-saja atas `*_history`) — review API bisa berubah, riwayatnya dicatat per `id`.
> - `categories`, `products`, `suppliers` — TRUNCATE + INSERT, tanpa riwayat (tabel kecil).

---

## Gold — tabel agregat siap pakai

**File:** `gold.sql`

Layer paling atas. Membuat **tabel agregat** (bukan view) yang siap dipakai
langsung oleh analis / dashboard:

| Tabel | Isi |
|---|---|
| `sales_summary` | Penjualan per tanggal, kategori, dan produk (unit, total sales, unique customers) |
| `customer_engagement` | Perilaku tiap customer: total order, total spend, order terakhir, avg order value |
| `product_performance` | Performa tiap produk: unit terjual, revenue, rating & jumlah review |
| `supplier_analysis` | Kontribusi tiap supplier: jumlah produk, unit terjual, revenue |
| `customer_reviews_summary` | Ringkasan rating per produk (avg, total, positive/negative) |

Beberapa hal yang diperhatikan di layer ini:

- **Tabel di-refresh tiap run** (`CREATE OR REPLACE TABLE`) supaya pipeline
  idempotent — dapat dijalankan ulang tanpa duplikasi.
- **Hanya data aktif yang dipakai** — gold hanya me-ref view aktif-saja /
  tabel full-refresh, yang memang tidak menyimpan versi lama. Tidak ada filter
  versi di gold karena tidak ada yang perlu difilter (struktural, bukan
  disiplin — lihat test `tests/test_scd_contract.py`).
- **Agregasi sebelum join** — fact dihitung dulu (via CTE) baru di-join,
  supaya produk dengan banyak order_items dan banyak review tidak menghasilkan
  baris berlipat (N×M).
- **`is_quarantined = FALSE`** — data kotor di silver tidak ikut masuk agregat.

**Tujuannya:** jawaban siap pakai untuk pertanyaan bisnis umum. Analis tidak
perlu menulis query kompleks atau paham SCD — cukup `SELECT * FROM gold.xxx`.

---

## transform.py — menjalankan semuanya

**File:** `transform.py`

Skrip orkestrasi paling sederhana: membuka `output/analytics.duckdb`, lalu
mengeksekusi `bronze.sql` → `silver.sql` → `gold.sql` secara berurutan.
Urutan ini penting karena tiap layer membaca dari layer di bawahnya.

Setelah selesai, skrip mencetak jumlah baris beberapa tabel di tiap layer
sebagai verifikasi cepat bahwa data mengalir sampai ke gold.
