# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Vũ Gia Khải |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602786 |
| GitHub | vukhai248 |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-02689 |

## Kết quả cuối

- `dataset_version` của bạn: `45a2c630a07d`  (khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **728 / 173 / 121** (tổng 1022 khung, 26 trip — train 18 trip, val 4 trip, test 4 trip)
- `report/verify.txt`: **27/27 kiểm tra đạt** (tạo bằng lần chạy `pipeline.cli verify`, lưu trong `report/verify.txt`).
- Ảnh chụp trong `report/screenshots/`: do máy làm bài chỉ có 7,86 GB RAM nên không chạy được Airflow webserver và Metabase (xem mục "Nhìn lại"). Ba ảnh airflow_graph, minio_lake, metabase_dashboard không có; phần A (27/27) và phần B (5 file bằng chứng) vẫn đầy đủ.

Ghi chú môi trường: pipeline được chạy **native trên Windows** (không Docker) bằng S3 server tương thích (moto), Postgres nhúng (pgserver) và FFmpeg portable, dùng đúng các lệnh của DAG qua `pipeline.cli`. Kết quả 27/27 và `dataset_version` giống hệt bản chạy trong container, vì `dataset_version` là băm nội dung (frame_id|split), không phụ thuộc môi trường chạy.

## Bước 1 · Bronze

### Câu 1
Ít nhất 4 khác biệt giữa telemetry nguồn A và nguồn B mà tôi thấy ở bronze:

1. **Tên cột khác hẳn**: A dùng `ts_utc, speed_mps, heading_deg, lat, lon, alt_m, acc_x, acc_y, gas, brake, steer`; B dùng `timestamp_ms, spd_kmh, hdg, latitude, longitude, altitude, ax, ay, throttle_pct, brake_pct, steering`.
2. **Đơn vị tốc độ**: A là **m/s** (`speed_mps`), B là **km/h** (`spd_kmh`) — phải chia 3.6 mới về cùng đơn vị.
3. **Kiểu thời gian**: A là **ISO-8601 UTC** (`2025-10-07T08:00:00.014Z`), B là **epoch milliseconds** (`1759933801509`) — phải chia 1000 rồi mới thành timestamp.
4. **Bàn đạp**: A đã là tỷ lệ 0..1 (`gas`, `brake`), B là **phần trăm** (`throttle_pct`, `brake_pct`) — phải chia 100.
5. **Độ lệch đồng hồ**: B khai báo `telemetry_clock_offset_ms = 1500` (đồng hồ thiết bị chạy trước video 1,5 giây); A là 0.

Pipeline giữ **mỗi nguồn một bảng bronze** (`bronze_raw_telemetry_a`, `bronze_raw_telemetry_b`) vì hai schema khác nhau về mặt cột; gộp vào một bảng lúc này sẽ buộc phải ép kiểu và đoán ý nghĩa từng cột ngay tại bronze — đúng lúc dữ liệu còn thô và chưa kiểm chứng. Để mọi cột ở dạng **chữ** (string) là quyết định của slide 14: bronze chỉ *ghi nguyên bản*, mọi quyết định về kiểu và đơn vị thuộc về staging, nơi có thể sửa luật mà không phải nạp lại dữ liệu. Bronze cũng giữ cột lineage (`_batch_id` = sha256 file, `_row_no`, `_ingested_at`, `_source_file`) để truy được dòng nào đến từ file nào.

Bằng chứng: bảng bronze có đúng **3021 dòng (A)** và **3234 dòng (B)** bằng số dòng CSV; video nhận diện bằng sha256 nên nạp hai lần liên tiếp vẫn báo `already present`; `lake.bronze_video_files` chỉ có 2 dòng với sha256 khớp file gốc.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema trong **staging** (`stg_telemetry_a.sql`, `stg_telemetry_b.sql`), cùng tên cột, cùng đơn vị (m/s, độ, mét, tỷ lệ 0..1) và cùng kiểu thời gian (timestamptz UTC). Với B cụ thể: `timestamp_ms` trừ đi `telemetry_clock_offset_ms` (đọc từ `bronze_video_files`) rồi chia 1000; `spd_kmh` chia 3.6; `throttle_pct`/`brake_pct` chia 100. Sau đó `silver_telemetry_classified` gán nhãn từng dòng (`duplicate` / `speed_missing` / `speed_out_of_range`) và `silver_telemetry` giữ lại dòng sạch, `silver_telemetry_rejects` giữ dòng lỗi.

Bằng chứng ở bước 2: bảng quarantine của tôi (đúng bằng số đếm độc lập từ CSV thô):

| drive | duplicate | speed_missing | speed_out_of_range | sạch | nhận về |
|---|---|---|---|---|---|
| drive_a | 20 | 30 | 3 | 2968 | 3021 |
| drive_b | 20 | 30 | 3 | 3181 | 3234 |

Tổng **106 dòng bị cách ly**, và `sạch + cách ly = nhận về` cho cả hai drive (2968+53=3021, 3181+53=3234). Sau chuẩn hoá, tốc độ tối đa của B là 100 km/h (hợp lý) và **thời điểm bắt đầu của B khớp thời điểm bắt đầu video** `2025-10-08 14:30:00` — chứng minh độ lệch đồng hồ 1,5 giây đã được trừ đúng.

Dòng lỗi vào `silver_telemetry_rejects` thay vì bị xoá vì: (1) xoá là mất thông tin không tái tạo được, trong khi cách ly giữ được **bằng chứng** và cho phép đối chiếu tổng số dòng; (2) mỗi dòng kèm `reject_reason` và `_batch_id`/`_row_no` nên truy được về file nguồn để sửa ở đầu vào; (3) đây là quyết định *vận hành* (có thể nới luật) chứ không phải quyết định *kỹ thuật* (dữ liệu sai).

### Câu 3
`max_gap_ms` là **warn** còn `in_range` là **error** vì hai luật trả lời hai câu hỏi khác nhau. `in_range(speed_mps, 0, 70)` kiểm tra **tính đúng đắn vật lý**: tốc độ 200 m/s là bất khả thi, là lỗi dữ liệu — nếu lọt vào gold thì mô hình học trên nhãn vô nghĩa, nên phải **dừng pipeline**. `max_gap_ms` (partition theo drive, ngưỡng 1000 ms) kiểm tra **tính đầy đủ**: cảm biến rơi mất tín hiệu vài trăm ms là chuyện bình thường trên đường thật, không làm dòng đó sai — chỉ làm join event-time kém chính xác hơn ở đoạn đó. Nó **báo cáo** nhưng không được phép chặn.

Thí nghiệm `break-gate` của tôi (`report/evidence/break_gate.json`): nới `max_speed_mps` từ 70 lên 1000, chạy `dbt test`:
- Luật bị bắt: **`in_range_silver_telemetry_speed_mps__70__0`, bắt được 6 dòng** (3 dòng của A + 3 dòng của B, đúng bằng số `speed_out_of_range` đếm được).
- Tóm tắt: lúc nới `PASS=5 WARN=1 ERROR=1`, khôi phục lại `PASS=6 WARN=1 ERROR=0` — pipeline trở lại xanh ngay.

Điều này cho thấy vai trò của cổng chất lượng: **cổng không tự sửa dữ liệu, nó chỉ chặn dòng chảy**. Nới luật thì dữ liệu xấu đi qua và test error chuyển đỏ, kéo theo mọi task phía sau (dbt_run_gold, export_dataset, publish_warehouse) không chạy. Nhờ vậy dữ liệu xấu **không thể lặng lẽ vào gold**.

## Bước 3 · Silver frames

### Câu 4
Theo `report/evidence/idempotency.json` (chạy lại partition `trip-00003` hai lần):
- Trước: **1246 dòng**, dấu vân tay `7b822c530dcf`
- Sau 2 lần chạy lại: **1246 dòng**, dấu vân tay `7b822c530dcf` → **KHÔNG ĐỔI**
- Cách sai (ghi `append` 2 lần cho một trip có 60 khung): **120 dòng** → nhân đôi đúng 2×.

Delta `overwrite` theo partition (`predicate = "trip_id = '...'"`) an toàn hơn xoá-rồi-ghi lại vì nó **thay thế nguyên tử trong một commit**: hoặc là commit thành công (bảng mới, version +1), hoặc là không có gì thay đổi (bảng giữ version cũ). Nếu ghép kiểu "xoá hết rồi ghi lại" thì giữa hai thao tác là một khoảng trống — lỗi mạng, tràn đĩa, hay process chết lúc đó sẽ để lại bảng **trống hoặc thiếu dòng**, và không có cách nào khôi phục. Với overwrite theo predicate, lỗi giữa chừng chỉ làm mất partition đang xử lý, các partition khác nguyên vẹn; chạy lại đúng partition đó là đủ, đúng như bài tập "task bị sập rồi chạy lại" ở bước 5. Ngoài ra mỗi lần chạy lại là một **version Delta mới** (time travel), nên có thể so sánh trước/sau thay vì chỉ tin vào log.

### Câu 5
Khung **đen** (`is_black`, `brightness < 12.0`) là camera không thu được tín hiệu — ảnh gần như một màu, không có thông tin gì để học. Khung **tối** (`is_dark`, `12.0 ≤ brightness < 40.0`) là cảnh thiếu sáng thật (đường hầm, bóng râm) nhưng **vẫn có nội dung**: vạch kẻ, đèn, vật thể. Pipeline xử lý khác nhau: **đen bị loại khỏi gold** (`gold_dataset_frames` lọc `not is_black`), còn **tối được gắn cờ `is_dark` nhưng vẫn giữ lại** — vì ban đêm và đường hầm là điều kiện vận hành có thật, loại chúng sẽ làm dataset lệch về ban ngày. Tôi đồng ý với cách xử lý này: đen là lỗi thiết bị (không có ground truth), tối là điều kiện khó (có ground truth, đáng học).

Theo `report/evidence/threshold.json` — số khung bị coi là trùng khi cho phép dHash khác tối đa 0 / 2 / 6 bit (ngưỡng pipeline dùng là **2**):

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 58 | 8 |
| **2 (đang dùng)** | **146** | **53** |
| 6 | 366 | 246 |

Tôi chọn giữ ngưỡng **2**, vì ở mức 0 (chỉ trùng tuyệt đối) thì khung đứng hình do camera dừng vẫn lọt qua (verify ghi nhận 11/12 khung bị bắt đúng), còn mở đến 6 bit thì loại tới 366/602 khung của A — quá nửa số khung, nhiều khả năng loại cả khung thật sự khác nhau (xe qua ngã tư, cảnh đổi). Ngưỡng 2 nằm giữa: bắt được khung camera đứng hình mà không cắt mất cảnh chuyển động chậm.

Ngưỡng này **phụ thuộc tốc độ xe**: cùng một khoảng thời gian, xe chạy 100 km/h trên cao tốc sẽ cho khung gần như khác hệt nhau (dự án ảnh ít bit), còn xe đỗ ở đèn đỏ sẽ cho khung gần như giống hệt dù cảnh vẫn đổi. Vì vậy ngưỡng cố định là phép **xấp xỉ**: nó hoạt động tốt khi tốc độ không đổi nhiều, nhưng trên dữ liệu hỗn hợp (đô thị nhiều lần dừng) sẽ có khung "trùng về mặt hash nhưng thực ra là cảnh khác". Thử nghiệm threshold cho thấy drive B (chạy nhanh hơn) ít khung trùng hơn hẳn A, đúng như dự đoán.

## Bước 4 · Gold

### Câu 6
Theo `report/evidence/leakage.json`, trên 1022 khung đáng dùng thuộc 26 trip:
- Chia **theo khung hình** (ngẫu nhiên 70/15/15 trên từng khung): **26/26 trip bị chia cắt**, và **26/26 trip có khung hình ở cả train lẫn test**.
- Chia **theo trip** (gold của pipeline): **0 trip bị chia cắt**.

Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo: các khung hình liền nhau trong một trip là **gần như cùng một cảnh** (2 khung/giây, camera chỉ dịch vài mét). Nếu một nửa khung của cảnh đó nằm ở train và nửa kia ở test, mô hình không hề "học được quy luật" — nó chỉ cần **nhớ lại** chính khung hình đã thấy. Kết quả là điểm test rất cao, nhưng khi gặp cảnh mới thật sự (trip khác) thì tụt ngay. Đó là rò rỉ dữ liệu (leakage), và chia theo trip là cách loại bỏ nó: mỗi cảnh chỉ thuộc về một split.

Bằng chứng gold của tôi: 26 trip, **không trip nào nằm ở hai split** (verify: `no trip is in two splits` PASS), mỗi drive đều có đủ train/val/test.

### Câu 7
`dataset_version` là **băm nội dung** (`sha256` của chuỗi `frame_id|split` của mọi khung, cắt 12 ký tự) chứ không phải ngày giờ hay số đếm, vì mục đích của nó là **nhận diện** dataset: hai lần chạy cho cùng một nội dung thì phải cùng một mã, và mã đó đổi ngay khi nội dung đổi. Nếu dùng ngày giờ thì hai lần chạy cùng dữ liệu vẫn ra hai mã khác — vô nghĩa cho việc đối chiếu; nếu dùng số đếm thì mã tăng dần nhưng không nói lên dataset *khác nhau chỗ nào*.

Hai lần chạy liên tiếp của tôi cho **cùng một kết quả**: `dataset_version` = `45a2c630a07d` ở cả lần `step 4` riêng và lần `step all` cuối, và `run_summary.json` ghi đúng mã đó.

`dataset_version` của tôi **khác** bạn bè dù cùng dữ liệu vì MSSV được trộm vào cách xếp trip: `gold_dataset_frames.sql` xếp hạng trip bằng `md5(trip_id || LAB_STUDENT_ID)` rồi chia theo tỷ lệ 70/15/15. MSSV đổi thì thứ tự trip đổi, thì **split của từng trip đổi**, thì chuỗi `frame_id|split` đổi, thì băm đổi. Đây là cơ chế chống chép bài: giảng viên chạy lại với MSSV của từng người và so `dataset_version`; mã không khớp nghĩa là bài không phải của người đó hoặc không chạy được. Mã `45a2c630a07d` là của riêng MSSV `2A202602786`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset của tôi, trả lời "dataset này được tạo từ dữ liệu nào?":

- **Từ các bảng Delta, kèm số version của từng bảng** tại thời điểm xuất: `bronze.raw_telemetry_a`, `bronze.raw_telemetry_b`, `bronze.trips_raw`, `bronze.video_files`, `silver.frames`, `silver.telemetry`, `silver.frames_flagged`, `silver.frame_telemetry` — mỗi bảng một số version Delta (time travel), nên biết được chính xác *dữ liệu ở phiên bản nào* đã tạo ra dataset.
- **Từ video nào**: `input_video_sha256` ghi sha256 của `drive_a.mp4` và `drive_b.mp4` — nhận diện video bằng nội dung, không bằng tên file.
- **Theo luật gì**: `rules` ghi rõ các khung bị loại (`is_black`, `is_blurry`, `is_near_dup`, không `sync_ok`) và cách chia (`per trip, stratified by drive, md5 order`).
- **Kèm**: số khung, số khung theo split, số trip theo split, `manifest_sha256`, `created_at`.

Nếu 3 tháng sau có người hỏi "tái tạo lại đúng dataset này", thông tin cần thiết là: **sha256 của hai video** (để có cùng đầu vào), **số version của 8 bảng Delta** (để biết pipeline đang ở trạng thái nào), **luật lọc và luật chia** (để cùng kết quả), và **`LAB_STUDENT_ID`** (vì nó thay đổi split). Nếu thiếu MSSV thì dù cùng video và cùng luật, split vẫn có thể khác và `dataset_version` sẽ không khớp.

## Bước 5 · Điều phối và phục vụ

### Câu 9
*(Phần này cần Airflow webserver để chứng minh bằng ảnh — máy làm bài 7,86 GB RAM không chạy được Airflow + Metabase cùng lúc, nên tôi trả lời phần cơ chế đã kiểm chứng được bằng code và bằng thí nghiệm, và nói rõ phần nào chưa có bằng chứng trực quan.)*

Cơ chế khi một task sập rồi chạy lại, theo code của lab:
- `frames.extract_trip(trip_id)` ghi **theo partition** bằng Delta `overwrite` với `predicate = "trip_id = '<trip>'"` (`pipeline/frames.py`). Task đỏ chỉ ảnh hưởng đúng **một** partition; 25 partition còn lại nguyên vẹn trong bảng.
- Vì DAG dùng `BashOperator.partial(task_id="silver_frames", max_active_tis_per_dagrun=1).expand(...)`, 26 task `silver_frames` là **26 task riêng biệt**, mỗi task một trip. Nên khi clear (làm mới) đúng task đỏ đó, chỉ task đó chạy lại — không phải chạy lại cả 26.
- Dữ liệu **không nhân đôi**: chạy lại ghi đè đúng partition bằng overwrite, và `_fail_once` chỉ làm crash ở **lần đầu** (ghi marker `.failed-<trip>` trong `work/`), lần thứ hai chạy thành công. Điều này khớp thí nghiệm `idempotency.json` của tôi: chạy lại 2 lần cho 1246 dòng và dấu vân tay không đổi.
- `dataset_version` **không đổi**: nó là băm của nội dung (`frame_id|split`), và partition được ghi lại với đúng cùng dữ liệu nên chuỗi băm giống hệt. Đó chính là điểm mạnh của việc version bằng nội dung thay vì bằng thời gian.

Bằng chứng trực quan (ảnh log task đỏ thấy trip, ảnh DAG xanh sau khi clear) **chưa có** vì không chạy được Airflow trên máy này. Trip của tôi theo MSSV là `trip-02689` (ghi ở đầu REPORT).

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**, ba thành phần vỡ đầu tiên:

1. **Lớp bronze — nhận dữ liệu (slide 12, ingestion).** 500 xe thay vì 2 nghĩa là số file tăng 250 lần. Điểm vỡ đầu tiên là *nhận diện trùng bằng sha256*: `_batch_seen()` phải **đọc toàn bộ bảng bronze** mỗi lần nạp để kiểm tra `_batch_id` đã có chưa — chi phí tăng theo số file đã nạp, thành O(n²) về lâu dài. Cần thay bằng **manifest/checkpoint bên ngoài** (bảng tra cứu `sha256 → đã nạp`, hoặc kiểm tra tồn tại của object trên S3 trước khi ghi) thay vì quét toàn bảng Delta.
2. **Lớp silver frames — decode video (slide 24).** Đây là nút cổ chai đã thấy rõ: 26 trip / 2 video tốn phần lớn thời gian của cả pipeline. 500 xe mỗi ngày là ~6500 video, mà `extract_trip` chạy **một tiến trình FFmpeg cho một trip** và ghi ảnh lên S3 **từng khung một** (mỗi `put_object` là một round-trip). Cần: (a) chạy song song theo worker thay vì tuần tự (`max_active_tis_per_dagrun=1` sẽ phải bỏ, hoặc tăng theo tài nguyên); (b) ghi theo **lô** (đóng gói nhiều khung một lần) thay vì từng object; (c) cân nhắc bộ nhớ đệm/thuật sampling thích ứng cho đoạn video không đổi.
3. **Lớp warehouse/dbt + DuckDB (slide 22, 23).** DuckDB trong thiết kế này là **một tiến trình ghi một file** (`threads: 1`, vì file `.duckdb` chỉ cho một writer) và dbt chạy **toàn bộ lại** các model mỗi lần. 500 xe/ngày nghĩa là bảng tăng nhanh, mỗi lần `dbt run` phải quét lại từ bronze. Cần thay bằng **incremental** (chỉ xử lý partition mới, dbt `incremental` materialization) và tách warehouse khỏi DuckDB file sang engine hỗ trợ ghi đồng thời (Postgres hoặc cloud warehouse) nếu số người đọc dashboard tăng.

Điểm chung của cả ba: pipeline này thiết kế cho **2 drive / 26 trip / bài lab**, nên mọi chỗ đều dùng "quét toàn bộ" hoặc "chạy tuần tự" — những lựa chọn hoàn toàn hợp lý ở quy mô nhỏ nhưng là thứ vỡ đầu tiên khi lên 250 lần.

## Câu hỏi riêng của bạn trên Metabase

*(Chưa làm được: Metabase cần Docker profile `bi` (~1,5 GB RAM) và máy làm bài 7,86 GB không đủ để chạy Metabase cùng lúc với pipeline. Câu SQL tôi dự định dùng trên `marts.mart_trip_summary`: so sánh `frames_kept / frames` theo từng trip để tìm trip có tỷ lệ khung bị loại cao nhất — lý do là trip đó có camera đứng hình hoặc đoạn thiếu sáng dài, từ đó biết chất lượng dữ liệu theo cảnh thay vì chỉ theo tổng số.)*

## Nhìn lại

Điều bất ngờ nhất: **ngưỡng chất lượng "đúng" là một phép xấp xỉ, không phải một con số tuyệt đối.** Ở câu 5 tôi cứ tưởng chọn ngưỡng trùng là chọn một con số, nhưng kết quả cho thấy nó phụ thuộc tốc độ xe — cùng ngưỡng 2 bit lại cho 146 khung trùng ở drive A nhưng chỉ 53 ở drive B. Và điều tương tự ở câu 3: luật nào là `error` hay `warn` không phải do luật đó "quan trọng hay không", mà do nó kiểm tra *tính đúng đắn* hay *tính đầy đủ*.

Điều còn chưa rõ: tôi chưa kiểm chứng được `sync_ok` ở mức phân bố — chỉ biết có 7 khung `sync_lost` ở mỗi drive (dưới ngưỡng warn 5%), nhưng chưa biết chúng tập trung ở đoạn cảm biến mất tín hiệu hay rải rác. Nếu tập trung thì join event-time của riêng đoạn đó thực ra không đáng tin, dù tổng thể vẫn đạt.

Một lỗi dữ liệu mà tôi nghĩ pipeline **chưa** bắt được: **khung bị nhiễu do mưa/tuyết hoặc bùn trên kính**. Các cờ hiện tại chỉ nhìn độ sáng trung bình (`brightness`), độ nét toàn cục (`blur_score`) và trùng lặp (`dhash`). Một khung có bùn loang trên kính vẫn có độ sáng bình thường và biên cạnh rõ (độ nét toàn cục cao), nên **không bị cờ nào bắt** — nhưng vùng dưới vết bùn thì mô hình không học được gì. Muốn bắt cần thêm tín hiệu về **phân bố không đều của độ nét trong khung** (độ lệch chuẩn của Laplacian theo vùng, thay vì phương sai toàn khung), và đó là thứ tôi sẽ thử nếu có thêm thời gian.
