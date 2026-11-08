# Tugas 2: Election by bullying

Implementasi Go menggunakan framework **mangos v3**, protokol REQ/REP, dan
transport TCP. Setiap node adalah proses tersendiri dan membuka satu socket REP
di `tcp://127.0.0.1:5000+ID`. Pesan dikirim sebagai JSON melalui socket REQ.
Source menggunakan module mangos di repository ini; tidak perlu `go mod init`
atau mengunduh mangos lagi.

## Menjalankan

Dari root repository, untuk Linux, macOS, atau Git Bash:

```sh
cd examples/bully
sh run.sh
```

Untuk Windows PowerShell:

```powershell
cd examples/bully
.\run.ps1
```

Jika Windows memblokir script karena execution policy, jalankan lewat proses
PowerShell sementara (tidak mengubah policy permanen):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\run.ps1
```

Script membangun program dan menjalankan node sesuai skenario tugas. Pada
PowerShell, `Start-Process -NoNewWindow` menjalankan node 0-6 di background
dengan output langsung di terminal yang sama. Node 7 langsung berhenti karena
mode `dead`. Script selesai, sementara node-node hidup tetap berjalan.

Untuk menghentikan semua proses bernama `bully` di Windows:

```powershell
Get-Process bully -ErrorAction SilentlyContinue | Stop-Process
```

Pada script Bash (`run.sh`), tekan **Ctrl+C** untuk menghentikan semua node.

Port 5000–5007 harus tersedia. Jangan jalankan dua demo pada port yang sama.

## Skenario tugas

1. Node 0, 1, 2, 3, 5, dan 6 memakai mode `live`.
2. Node 7 memakai mode `dead`: langsung berhenti tanpa membuka socket.
3. Node 4 memakai mode `live_init`: menunggu dua detik agar proses lain siap,
   kemudian mengirim `PING` ke coordinator awal (node 7). Bila tidak mendapat
   `PONG` dalam batas waktu, node 4 memulai election.
4. Node 4 mengirim `ELECTION` ke 5, 6, dan 7. Node 5 dan 6 membalas `OK`.
   Sesudah menghubungi semua higher-up, node 4 diam menunggu `COORDINATOR`.
5. Setelah membalas `OK`, node 5 memulai election ke 6 dan 7; node 6 membalas
   `OK`, sehingga node 5 juga diam menunggu pengumuman.
6. Node 6 mengirim `ELECTION` ke 7. Karena tidak ada balasan, node 6 menjadi
   coordinator dan mengirim `COORDINATOR` ke **0, 1, 2, 3, 4, dan 5**.

Node 0 disertakan karena ID-nya lebih kecil daripada 6, sesuai gambar slide.
Election node 5 dan 6 berjalan **konkuren** pada goroutine masing-masing.
Urutan log antarproses dapat berbeda dari urutan daftar di atas. Dalam satu
putaran, satu node menghubungi semua ID lebih tinggi secara berurutan. Socket
penerima tetap aktif untuk membalas pesan ketika election berlangsung.

Cuplikan log penting (timestamp dan baris lain dihilangkan):

```text
[node 4] coordinator 7 tidak merespons; mulai election
[node 4] ELECTION -> 5
[node 5] ELECTION <- 4; OK -> 4
[node 4] OK <- 5
[node 5] ELECTION -> 6
[node 6] ELECTION <- 5; OK -> 5
[node 6] ELECTION -> 7
[node 6] node 7 tidak menjawab ELECTION
[node 6] tidak ada OK dari ID lebih tinggi; MENJADI COORDINATOR
[node 6] COORDINATOR -> 0
...
[node 4] COORDINATOR <- 6; coordinator sekarang: 6
```

## Menjalankan proses secara manual

Di folder ini:

```sh
go build bully.go
./bully 0 live &
./bully 1 live &
./bully 2 live &
./bully 3 live &
./bully 4 live_init &
./bully 5 live &
./bully 6 live &
./bully 7 dead
```

Argumen mode boleh dihilangkan: default node 4 adalah `live_init`, node 7
adalah `dead`, dan sisanya `live` (untuk konfigurasi delapan node).
Pada Windows, binary bernama `bully.exe`. Untuk mode eksplisit, `live` hanya
menjawab dan ikut election saat diminta, `live_init` juga mengecek coordinator
awal, sedangkan `dead` tidak menjawab pesan apa pun.

Opsi harus ditulis **sebelum ID**, dengan nilai yang sama pada semua proses:

```sh
./bully -base-port 6000 -timeout 1s -startup 3s 4 live_init
```

`-nodes` mengatur jumlah node (default 8); coordinator awal diasumsikan ID
tertinggi, yaitu `N-1`. `-timeout` membatasi setiap operasi kirim/terima
(default 800 ms). Sesudah menerima `OK`, node menunggu pengumuman maksimal
`4 × N × timeout`, lalu mengulang election jika pengumuman tidak datang.

Ini demo satu mesin dengan daftar anggota tetap. Pengecekan coordinator
dilakukan sekali saat `live_init` mulai, bukan heartbeat berkala. Deteksi
kegagalan memakai timeout; diasumsikan jaringan lokal stabil dan startup cukup
lama untuk semua node siap. Pemulihan node dan partisi jaringan di luar skenario.

## Verifikasi

```sh
go test ./examples/bully -v
```

Jalankan perintah tes dari root repository. Tes membuka socket mangos TCP asli
untuk memastikan semua node hidup memilih coordinator 6 ketika 7 mati,
coordinator 5 ketika 6 dan 7 mati, coordinator 4 ketika 5–7 mati, serta
coordinator 7 ketika semua node hidup.

Referensi pola komunikasi: [dokumentasi resmi mangos REQ/REP](https://nanomsg.org/getting-started/mangos/reqrep.html).
