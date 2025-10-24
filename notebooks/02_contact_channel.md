```python
#init spark
!pip install -q pyspark findspark

import findspark
findspark.init()
import pyspark
findspark.find()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("analytics_data").getOrCreate()

```


```python
path = "../data/bank-additional/bank-additional-full.csv"
raw_df = (
    spark.read
    .option("header", True)
    .option("inferschema", True)
    .option("sep", ";")
    .csv(path)
)

cols = ['contact', 'month', 'day_of_week', 'duration', 'campaign', 'pdays', 'previous']
target_col = 'y'
df = raw_df.select(*(cols + [target_col]))

df.show(10, truncate=False)
```

    +---------+-----+-----------+--------+--------+-----+--------+---+
    |contact  |month|day_of_week|duration|campaign|pdays|previous|y  |
    +---------+-----+-----------+--------+--------+-----+--------+---+
    |telephone|may  |mon        |261     |1       |999  |0       |no |
    |telephone|may  |mon        |149     |1       |999  |0       |no |
    |telephone|may  |mon        |226     |1       |999  |0       |no |
    |telephone|may  |mon        |151     |1       |999  |0       |no |
    |telephone|may  |mon        |307     |1       |999  |0       |no |
    |telephone|may  |mon        |198     |1       |999  |0       |no |
    |telephone|may  |mon        |139     |1       |999  |0       |no |
    |telephone|may  |mon        |217     |1       |999  |0       |no |
    |telephone|may  |mon        |380     |1       |999  |0       |no |
    |telephone|may  |mon        |50      |1       |999  |0       |no |
    +---------+-----+-----------+--------+--------+-----+--------+---+
    only showing top 10 rows
    
    


```python
print("schema df: ")
df.printSchema()
print("__________________________________________")

print("\n So dong df: ")
print(df.count())
print("__________________________________________")

```

    schema df: 
    root
     |-- contact: string (nullable = true)
     |-- month: string (nullable = true)
     |-- day_of_week: string (nullable = true)
     |-- duration: integer (nullable = true)
     |-- campaign: integer (nullable = true)
     |-- pdays: integer (nullable = true)
     |-- previous: integer (nullable = true)
     |-- y: string (nullable = true)
    
    __________________________________________
    
     So dong df: 
    41188
    __________________________________________
    


```python
print("Checking missing values: ")
total = df.count()

from pyspark.sql import functions as F
for c in df.columns:
    miss = df.filter(F.col(c).isNull()).count()
    print(f"{c:20s} null = {miss}")
```

    Checking missing values: 
    contact              null = 0
    month                null = 0
    day_of_week          null = 0
    duration             null = 0
    campaign             null = 0
    pdays                null = 0
    previous             null = 0
    y                    null = 0
    

### * Tỷ lệ phân bố giữa các biến


```python

cat_cols = ['contact', 'month', 'day_of_week', 'y']
for c in cat_cols:
    fre_val = df.groupBy(c).count().orderBy('count', ascending = False)
 
    total = df.count()
    col_expr = ((F.col('count') / total) * 100).cast("double")
    fre_val.withColumn("ratio (%)", F.round(col_expr, 2)).show()

```

    +---------+-----+---------+
    |  contact|count|ratio (%)|
    +---------+-----+---------+
    | cellular|26144|    63.47|
    |telephone|15044|    36.53|
    +---------+-----+---------+
    
    +-----+-----+---------+
    |month|count|ratio (%)|
    +-----+-----+---------+
    |  may|13769|    33.43|
    |  jul| 7174|    17.42|
    |  aug| 6178|     15.0|
    |  jun| 5318|    12.91|
    |  nov| 4101|     9.96|
    |  apr| 2632|     6.39|
    |  oct|  718|     1.74|
    |  sep|  570|     1.38|
    |  mar|  546|     1.33|
    |  dec|  182|     0.44|
    +-----+-----+---------+
    
    +-----------+-----+---------+
    |day_of_week|count|ratio (%)|
    +-----------+-----+---------+
    |        thu| 8623|    20.94|
    |        mon| 8514|    20.67|
    |        wed| 8134|    19.75|
    |        tue| 8090|    19.64|
    |        fri| 7827|     19.0|
    +-----------+-----+---------+
    
    +---+-----+---------+
    |  y|count|ratio (%)|
    +---+-----+---------+
    | no|36548|    88.73|
    |yes| 4640|    11.27|
    +---+-----+---------+
    
    

### Nhận xét phân tích tần suất các biến phân loại

#### 1. `contact` — Kênh liên hệ
Tỷ lệ **cellular (63.5%)** cao hơn **telephone (36.5%)**.  
Điều này phản ánh đúng thực tế giai đoạn 2008–2010, khi ngân hàng chuyển dần sang gọi qua **di động** để tiếp cận khách hàng nhanh hơn.  

Khi phân tích hiệu quả chiến dịch, cần kiểm tra xem **tỷ lệ “yes”** giữa hai kênh này có khác biệt đáng kể không.  
Nếu di động có tỷ lệ thành công cao hơn, đó là tín hiệu cho việc **ưu tiên kênh liên hệ** này trong các chiến dịch sau.

---

#### 2. `month` — Tháng gọi
Các tháng có nhiều cuộc gọi nhất:
- **May (33%)**
- **July (17%)**
- **August (15%)**
- **June (13%)**

→ Chiếm hơn **75% tổng số cuộc gọi**.  
Có thể đây là **chiến dịch mùa hè**, khi ngân hàng đẩy mạnh huy động vốn hoặc tung sản phẩm mới.

Các tháng khác (Oct, Sep, Mar, Dec) có tỷ lệ rất nhỏ → thường là **off-campaign** hoặc **chiến dịch thử nghiệm**.  
Nên kiểm tra thêm xem **tỷ lệ “yes” có biến động theo mùa** không; ví dụ: tháng May gọi nhiều nhưng hiệu quả có thể không cao.

---

#### 3. `day_of_week` — Ngày gọi
Phân bố **khá đồng đều (khoảng 19–21%)** mỗi ngày.  
Điều này cho thấy ngân hàng triển khai chiến dịch đều trong tuần, **không tập trung riêng vào đầu hoặc cuối tuần**.

Đây là đặc điểm tốt, giúp **loại bỏ bias theo ngày** khi huấn luyện mô hình.  
Tuy nhiên, vẫn nên xem thử **thứ Sáu** có tỷ lệ “yes” cao hơn không — vì khách hàng cuối tuần có thể tâm lý thoải mái hơn.

---

#### 4. `y` — Biến mục tiêu (kết quả)
Dữ liệu **rất mất cân bằng**:  
- **Yes:** 11.3%  
- **No:** 88.7%

Đây là đặc trưng nổi tiếng của **Bank Marketing Dataset**.  
Hệ quả: nếu huấn luyện mô hình mà **không xử lý imbalance**, mô hình sẽ thiên về dự đoán “no”.

→ Ở bước mô hình hóa, cần **cân bằng lớp** bằng:
- `class_weighting`
- `SMOTE`
- hoặc `resampling`

để đảm bảo mô hình học được tín hiệu thực sự của nhóm “yes”.



```python
#Kiem tra tan suat phan bo cua cac bien so
num_cols = ['duration', 'campaign', 'pdays', 'previous']

df.select(num_cols).describe().show()

percentiles = df.approxQuantile(num_cols, [0.25, 0.5, 0.75, 0.9, 0.95, 0.99], 0.01)
print(percentiles, "\n")


for i, c in enumerate(num_cols):
    res = dict(zip(["25%", "50%", "75%", "90%", "95%", "99%"], percentiles[i]))
    print(f"Percentile cua {c}: ")
    print(res, "\n")
```

    +-------+------------------+-----------------+-----------------+-------------------+
    |summary|          duration|         campaign|            pdays|           previous|
    +-------+------------------+-----------------+-----------------+-------------------+
    |  count|             41188|            41188|            41188|              41188|
    |   mean| 258.2850101971448|2.567592502670681|962.4754540157328|0.17296299893172767|
    | stddev|259.27924883646455|2.770013542902331|186.9109073447414|0.49490107983928927|
    |    min|                 0|                1|                0|                  0|
    |    max|              4918|               56|              999|                  7|
    +-------+------------------+-----------------+-----------------+-------------------+
    
    [[102.0, 177.0, 312.0, 526.0, 702.0, 4918.0], [1.0, 2.0, 3.0, 5.0, 6.0, 56.0], [999.0, 999.0, 999.0, 999.0, 999.0, 999.0], [0.0, 0.0, 0.0, 1.0, 1.0, 7.0]] 
    
    Percentile cua duration: 
    {'25%': 102.0, '50%': 177.0, '75%': 312.0, '90%': 526.0, '95%': 702.0, '99%': 4918.0} 
    
    Percentile cua campaign: 
    {'25%': 1.0, '50%': 2.0, '75%': 3.0, '90%': 5.0, '95%': 6.0, '99%': 56.0} 
    
    Percentile cua pdays: 
    {'25%': 999.0, '50%': 999.0, '75%': 999.0, '90%': 999.0, '95%': 999.0, '99%': 999.0} 
    
    Percentile cua previous: 
    {'25%': 0.0, '50%': 0.0, '75%': 0.0, '90%': 1.0, '95%': 1.0, '99%': 7.0} 
    
    

### Nhận xét phân tích các biến số

#### 1. `duration` — Thời lượng cuộc gọi
- Trung bình khoảng **258 giây**, độ lệch chuẩn gần bằng trung bình → **phân bố rất lệch phải (right-skewed)**.  
- **75%** cuộc gọi kéo dài ≤ **312 giây**, nhưng **1% trên cùng** lên tới gần **5000 giây (~82 phút)** → có **outlier mạnh**.  
- Điều này hợp lý, vì đa số khách hàng từ chối sớm; còn các cuộc gọi dài thường đến từ khách hàng có hứng thú hoặc nhân viên thuyết phục lâu.  
- Tuy nhiên, như đã nói ở phần trước, `duration` **không nên dùng để dự đoán trực tiếp**, vì đây là **kết quả sau khi gọi**, không phải thông tin biết trước.  
  → Dùng trong **EDA (phân tích khám phá dữ liệu)** để hiểu hành vi khách hàng là hợp lý.



#### 2. `campaign` — Số lần liên hệ trong chiến dịch hiện tại
- **Trung vị = 2**, **75% ≤ 3**, nhưng **1% cao nhất** lên tới **56 lần** → có những khách hàng bị gọi **hơn 50 lần (!)**  
- Đây là **outlier rõ ràng**, nhưng lại chứa **thông tin hành vi**: chiến dịch đã cố gắng “đuổi theo” khách hàng đó.  
- Khi mô hình hóa, nên:
  - **Giới hạn (clip)** giá trị tối đa ở một ngưỡng hợp lý (ví dụ 10), hoặc  
  - **Log-transform** để giảm ảnh hưởng của các giá trị cực lớn.



#### 3. `pdays` — Số ngày từ lần liên hệ trước
- Tất cả các percentile đều = **999**, cho thấy **phần lớn khách hàng chưa từng được liên hệ trước đây**.  
- Theo mô tả dữ liệu, **999 nghĩa là “no previous contact”**.  
- Biến này ít thông tin trực tiếp, nên khi xử lý có thể:
  - Tạo biến nhị phân mới: `has_contact_before = (pdays != 999)`, hoặc  
  - Giữ nguyên 999 và để mô hình tự học (tùy thuật toán).



#### 4. `previous` — Số lần liên hệ trước chiến dịch hiện tại
- **75% khách hàng chưa từng được liên hệ (0)**, **95% ≤ 1**, chỉ vài người đến **7 lần**.  
- Nghĩa là **đa số là khách hàng mới**.  
- Biến này có **phân bố rất lệch (one-sided)**, nên khi đưa vào mô hình cần:
  - **Chuẩn hóa (scaling)** hoặc  
  - **Binning (phân nhóm)** để mô hình dễ học và ổn định hơn.
---

### 1. Insight về hành vi liên hệ (kênh – thời điểm – kết quả):

#### 1.1 Về kênh liên hệ 
→ Mục tiêu: tìm hiểu kênh và thời điểm nào hiệu quả nhất.
**Câu hỏi khai thác:**

- Kênh cellular có giúp tăng tỷ lệ phản hồi tích cực không?

- Liệu telephone có thể bị loại bỏ hoặc giảm tần suất để tiết kiệm chi phí?


```python
#Tỷ lệ khách hàng "yes" của từng kênh
cellular_total = df.filter(df['contact'] == 'cellular').count()
yes_cellular = df.filter((df['contact'] == 'cellular') & (df['y'] == 'yes')).count()
cellular_ratio = round(yes_cellular / cellular_total * 100, 2)
print(f"Ty le khách hang say yes trong cellular la:{cellular_ratio}% tren tong so {cellular_total} \n")



telephone_total = df.filter(df['contact'] == 'telephone').count()
yes_telephone = df.filter((df['contact'] == 'telephone') & (df['y'] == 'yes')).count()
telephone_ratio = round(yes_telephone / telephone_total * 100, 2)
print(f"ty le  khach hang say yes khi su dung telephone la:{telephone_ratio}% tren tong so {telephone_total} ")


```

    Ty le khách hang say yes trong cellular la:14.74% tren tong so 26144 
    
    ty le  khach hang say yes khi su dung telephone la:5.23% tren tong so 15044 
    

Phân tích cho thấy **kênh di động (cellular)** có tỷ lệ khách hàng đồng ý gửi tiết kiệm **cao gấp gần 3 lần** so với **điện thoại bàn (telephone)**.

- **Cellular:** 14.74% khách hàng đồng ý trên tổng **26,144 cuộc gọi**.  
- **Telephone:** 5.23% khách hàng đồng ý trên tổng **15,044 cuộc gọi**.

👉 **Kết luận:**  
Kênh **di động** cho thấy **hiệu quả vượt trội**, có thể do khách hàng **dễ tiếp cận hơn** và **phản hồi nhanh hơn**.  
**Đề xuất:** Ngân hàng nên **ưu tiên ngân sách và nhân sự cho kênh di động**, đồng thời **giảm tần suất gọi qua điện thoại bàn** để **tối ưu chi phí và nâng cao tỷ lệ chuyển đổi**.

---

#### 1.2 Về tháng gọi (month)

→ **Mục tiêu:** kiểm tra xem yếu tố **mùa vụ** có ảnh hưởng đến tỷ lệ khách hàng đồng ý (“yes”) hay không.

**Câu hỏi khai thác:**

- Có phải **tháng May** tuy có nhiều cuộc gọi nhưng **tỷ lệ thành công lại thấp**?

- **Tháng March** hoặc **September** có thể là “**thời điểm vàng**” – ít cuộc gọi nhưng **hiệu quả cao hơn**?

- Có **mối quan hệ nào giữa số lượng cuộc gọi và tỷ lệ thành công** (gọi nhiều chưa chắc tốt hơn)?



```python
month_df = (
    df.groupBy('month')
      .agg(
          F.count('*').alias('total'),
          F.sum((F.col('y') == 'yes').cast('int')).alias('yes_count')
      )
      .withColumn('yes_ratio (%)', F.round(F.col('yes_count')/F.col('total')* 100, 2))
      .orderBy('yes_ratio (%)')
)
month_df.show()
```

    +-----+-----+---------+-------------+
    |month|total|yes_count|yes_ratio (%)|
    +-----+-----+---------+-------------+
    |  may|13769|      886|         6.43|
    |  jul| 7174|      649|         9.05|
    |  nov| 4101|      416|        10.14|
    |  jun| 5318|      559|        10.51|
    |  aug| 6178|      655|         10.6|
    |  apr| 2632|      539|        20.48|
    |  oct|  718|      315|        43.87|
    |  sep|  570|      256|        44.91|
    |  dec|  182|       89|         48.9|
    |  mar|  546|      276|        50.55|
    +-----+-----+---------+-------------+
    
    

Phân tích cho thấy các chiến dịch được triển khai mạnh nhất vào **tháng May, July và August**, nhưng **tỷ lệ thành công lại khá thấp**.  
Ngược lại, những tháng có **ít cuộc gọi** như **March, September, October và December** lại có tỷ lệ “yes” **rất cao** — thậm chí **gấp 5–8 lần so với tháng May**.

- **Tháng May:** 6.43% khách hàng đồng ý trên tổng **13,769 cuộc gọi**.  
- **Tháng August:** 10.6% khách hàng đồng ý trên tổng **6,178 cuộc gọi**.  
- **Tháng April:** 20.48% khách hàng đồng ý trên tổng **2,632 cuộc gọi**.  
- **Tháng March:** 50.55% khách hàng đồng ý trên tổng **546 cuộc gọi**.

👉 **Kết luận:**  
Hiệu quả chiến dịch có **tính mùa vụ rõ rệt**.  
Những tháng ngân hàng **gọi nhiều (đặc biệt là May, July)** lại có **tỷ lệ phản hồi thấp**,  
trong khi các tháng **ít gọi (March, September, October, December)** mang lại **tỷ lệ thành công vượt trội**.

**Đề xuất:**  
Ngân hàng nên **tái phân bổ lịch gọi**, **tăng cường chiến dịch** vào các tháng có hiệu quả cao,  
đồng thời **giảm tần suất** ở các tháng thấp hiệu quả như **May–July** để **nâng cao hiệu suất tổng thể**.

---

#### 1.3 Về ngày trong tuần (day_of_week)

→ **Mục tiêu:** xác định **ngày nào trong tuần** mang lại **tỷ lệ khách hàng đồng ý cao nhất**,  
từ đó hỗ trợ ngân hàng **lên lịch gọi tối ưu** cho đội ngũ tư vấn.

**Câu hỏi khai thác:**

- Liệu khách hàng có xu hướng **đồng ý nhiều hơn vào cuối tuần** (thứ Năm, thứ Sáu) khi **tâm lý thoải mái hơn**?

- Có **sự khác biệt rõ** giữa **đầu tuần** và **cuối tuần** hay không?

- Ngân hàng có thể **ưu tiên gọi vào các ngày “hiệu quả” hơn** để **tăng tỷ lệ chuyển đổi**?



```python
dayOfWeek_df = (
    df.groupBy('day_of_week')
      .agg(
         F.count('*').alias('total'),
         F.sum((F.col('y') == 'yes').cast('int')).alias('yes_count')
      )
      .withColumn('yes_ratio(%)', F.round((F.col('yes_count')/F.col('total') * 100), 2))
      .orderBy('yes_ratio(%)',ascending = False)
)
dayOfWeek_df.show()
```

    +-----------+-----+---------+------------+
    |day_of_week|total|yes_count|yes_ratio(%)|
    +-----------+-----+---------+------------+
    |        thu| 8623|     1045|       12.12|
    |        tue| 8090|      953|       11.78|
    |        wed| 8134|      949|       11.67|
    |        fri| 7827|      846|       10.81|
    |        mon| 8514|      847|        9.95|
    +-----------+-----+---------+------------+
    
    

Phân tích cho thấy **tỷ lệ khách hàng “say yes” cao nhất** rơi vào **thứ Năm (12.12%)**,  
tiếp theo là **thứ Ba (11.78%)** và **thứ Tư (11.67%)**.  
Trong khi đó, **thứ Hai có tỷ lệ thấp nhất (9.95%)** — tức là **đầu tuần khách hàng ít phản hồi tích cực hơn**.

- **Thứ Năm:** 12.12% khách hàng đồng ý trên tổng **8,623 cuộc gọi**.  
- **Thứ Ba:** 11.78% khách hàng đồng ý trên tổng **8,090 cuộc gọi**.  
- **Thứ Hai:** 9.95% khách hàng đồng ý trên tổng **8,514 cuộc gọi**.

👉 **Kết luận:**  
Tỷ lệ phản hồi tích cực có **xu hướng tăng dần về giữa và cuối tuần**, đạt **đỉnh vào thứ Năm**.  
Điều này gợi ý rằng **thời điểm giữa – cuối tuần** là **“khung giờ vàng” để triển khai cuộc gọi**,  
khi khách hàng có **tâm lý thoải mái** và **sẵn sàng tương tác hơn**.

---

### 2: “Tần suất & lịch sử liên hệ.”

#### 2.1. Tần suất gọi trong chiến dịch hiện tại – campaign
→ **Mục tiêu:** xem việc **gọi nhiều hơn trong cùng một chiến dịch** có làm **tăng khả năng khách hàng “yes”** hay không.

**Cần phân tích:**

- So sánh **trung bình số lần gọi (campaign)** giữa nhóm **“yes”** và **“no”**.  
- Tính **tỷ lệ “yes” theo nhóm tần suất gọi**, ví dụ: **1–2**, **3–5**, **>5 lần**.

**Câu hỏi khai thác:**

- Liệu **gọi quá nhiều có phản tác dụng** không?  
- Có tồn tại **ngưỡng “số lần gọi tối ưu”** nào cho chiến dịch?


```python
# Trung bình số lần gọi giữa hai nhóm yes / no
avg_calls = df.groupBy("y").agg(F.round(F.avg("campaign"), 2).alias("avg_calls"))
avg_calls.show()

# Phân nhóm tần suất gọi: ít - trung bình - nhiều
campaign_gr = (
    df.withColumn(
        'campaign_group',
        F.when(F.col('campaign') <= 2, "low")
        .when(F.col('campaign') <= 5, "medium")
        .otherwise("high")
    )
    .groupBy('campaign_group')
    .agg(
        F.count("*").alias("total"),
        F.sum((F.col("y") == "yes").cast("int")).alias("yes_count")
    )
    .withColumn("yes_ratio (%)", F.round(F.col("yes_count") / F.col("total") * 100, 2))
    .orderBy("campaign_group")
)

campaign_gr.show()
```

    +---+---------+
    |  y|avg_calls|
    +---+---------+
    | no|     2.63|
    |yes|     2.05|
    +---+---------+
    
    +--------------+-----+---------+-------------+
    |campaign_group|total|yes_count|yes_ratio (%)|
    +--------------+-----+---------+-------------+
    |          high| 3385|      186|         5.49|
    |           low|28212|     3511|        12.45|
    |        medium| 9591|      943|         9.83|
    +--------------+-----+---------+-------------+
    
    

Phân tích cho thấy những khách hàng **đồng ý gửi tiết kiệm (“yes”)** chỉ được gọi trung bình **2.05 lần**,  
trong khi nhóm **từ chối (“no”)** được gọi trung bình **2.63 lần**.  
Điều này gợi ý rằng **gọi quá nhiều lần không giúp tăng hiệu quả**, thậm chí **có xu hướng phản tác dụng**.

**Phân nhóm tần suất gọi:**

- **Nhóm ít (≤ 2 lần):** tỷ lệ thành công **12.45%** trên tổng **28,212 cuộc gọi**.  
- **Nhóm trung bình (3–5 lần):** tỷ lệ thành công **9.83%** trên tổng **9,591 cuộc gọi**.  
- **Nhóm cao (>5 lần):** tỷ lệ thành công chỉ **5.49%** trên tổng **3,385 cuộc gọi**.

👉 **Kết luận:**  
Tần suất gọi cao **không làm tăng tỷ lệ đồng ý**; ngược lại, **càng gọi nhiều khách hàng càng ít phản hồi tích cực**.  
Điều này cho thấy **nhiều khách hàng có thể cảm thấy phiền** khi bị liên hệ lặp lại trong cùng chiến dịch.

**Đề xuất:**  
Ngân hàng nên **giới hạn số lần gọi tối đa ở mức 2–3 lần mỗi chiến dịch**,  
và **chuyển trọng tâm sang chất lượng cuộc gọi thay vì tần suất**,  
nhằm **tối ưu hiệu quả và giảm chi phí nhân sự**.

---

#### 2.2. Lịch sử liên hệ – `pdays` và `has_contact_before`

→ **Mục tiêu:** tìm hiểu **tác động của việc tái liên hệ** với khách hàng **đã từng được gọi trước đây**.

**Cần phân tích:**

- So sánh **tỷ lệ “yes”** giữa hai nhóm:  
  - **Đã từng được gọi trước** (`pdays ≠ 999`)  
  - **Chưa từng được gọi** (`pdays = 999`)  
- Kiểm tra xem **khoảng cách giữa hai lần liên hệ** (`pdays` nhỏ → gọi gần đây) có **ảnh hưởng đến tỷ lệ “yes”** hay không.

**Câu hỏi khai thác:**

- Việc **gọi lại** có giúp **tăng cơ hội thành công** không?  
- Nếu có, **sau bao lâu gọi lại** là hợp lý?



```python
# Tạo biến đánh dấu đã từng liên hệ trước đó
df_pdays = df.withColumn("has_contact_before", (F.col("pdays") != 999).cast("int"))
total_count = df_pdays.count()

# Đếm số lượng và tỷ lệ "yes" theo nhóm liên hệ
pdays_stats = (
    df_pdays.groupBy("has_contact_before")
    .agg(
        F.count("*").alias("total"),
        F.sum((F.col("y") == "yes").cast("int")).alias("yes_count")
    )
    .withColumn("yes_ratio (%)", F.round(F.col("yes_count") / F.col("total") * 100, 2))
    .withColumn("group_ratio (%)", F.round(F.col("total") / total_count * 100, 2))
    .orderBy("has_contact_before")
)
pdays_stats.show()

# Chỉ giữ những khách hàng đã từng được liên hệ (pdays != 999)
df_recontacted = df.filter(F.col("pdays") != 999)

# Phân nhóm khoảng cách gọi lại
pdays_grouped = (
    df_recontacted.withColumn(
        "pdays_group",
        F.when(F.col("pdays") <= 5, "≤ 5 ngày")
         .otherwise("> 5 ngày")
    )
    .groupBy("pdays_group")
    .agg(
        F.count("*").alias("total"),
        F.sum((F.col("y") == "yes").cast("int")).alias("yes_count")
    )
    .withColumn("yes_ratio (%)", F.round(F.col("yes_count") / F.col("total") * 100, 2))
    .orderBy("pdays_group")
)
pdays_grouped.show()

```

    +------------------+-----+---------+-------------+---------------+
    |has_contact_before|total|yes_count|yes_ratio (%)|group_ratio (%)|
    +------------------+-----+---------+-------------+---------------+
    |                 0|39673|     3673|         9.26|          96.32|
    |                 1| 1515|      967|        63.83|           3.68|
    +------------------+-----+---------+-------------+---------------+
    
    +-----------+-----+---------+-------------+
    |pdays_group|total|yes_count|yes_ratio (%)|
    +-----------+-----+---------+-------------+
    |   > 5 ngày|  810|      522|        64.44|
    |   ≤ 5 ngày|  705|      445|        63.12|
    +-----------+-----+---------+-------------+
    
    

Phân tích cho thấy 
- **Khách hàng từng được liên hệ trước đó** có **tỷ lệ đồng ý rất cao (~63.8%)**,  
cao gấp nhiều lần so với nhóm **chưa từng được gọi (9.3%)**.  

- Giữa nhóm **được gọi lại trong vòng 5 ngày** và **sau 5 ngày**, tỷ lệ “yes” gần như **tương đương (63–64%)**.

- Chỉ **3.68%** khách hàng từng được gọi trước đó, nghĩa là **hơn 96%** là lần đầu tiên được liên hệ.  Điều này xác nhận chiến dịch marketing của ngân hàng chủ yếu là **cold call**, không phải **chăm sóc khách hàng cũ**.  

👉 **Kết luận:**  
**Tái liên hệ khách hàng cũ** là một **chiến lược hiệu quả rõ rệt**,  
trong khi **khoảng cách giữa hai lần gọi không ảnh hưởng đáng kể**.

**Đề xuất:**  
Ngân hàng nên **ưu tiên xây dựng chiến dịch follow-up có chọn lọc**  
thay vì **tập trung quá nhiều vào gọi mới (cold call)**.

---

#### 2.3. Số lần liên hệ trong các chiến dịch trước – `previous`

→ **Mục tiêu:** đánh giá xem **lịch sử được gọi nhiều lần trước đây** có giúp **tăng xác suất khách hàng “yes”** trong chiến dịch hiện tại hay không.

**Cần phân tích:**

- Tính **tỷ lệ “yes” theo giá trị `previous`** (0, 1, 2, …).  
- Kiểm tra xem **khách hàng từng được liên hệ 1–2 lần trước đó** có **dễ đồng ý hơn** so với nhóm chưa từng liên hệ không.

**Câu hỏi khai thác:**

- “**Khách hàng quen thuộc**” có thực sự **tiềm năng hơn** không?  
- Có nên **tập trung vào nhóm đã liên hệ nhiều lần trong quá khứ** để tăng hiệu quả chiến dịch?



```python
from pyspark.sql import functions as F

# Tính tỷ lệ yes theo số lần được liên hệ trong các chiến dịch trước
previous_stats = (
    df.groupBy("previous")
      .agg(
          F.count("*").alias("total"),
          F.sum((F.col("y") == "yes").cast("int")).alias("yes_count")
      )
      .withColumn("yes_ratio (%)", F.round(F.col("yes_count") / F.col("total") * 100, 2))
      .orderBy("previous")
)

previous_stats.show()

```

    +--------+-----+---------+-------------+
    |previous|total|yes_count|yes_ratio (%)|
    +--------+-----+---------+-------------+
    |       0|35563|     3141|         8.83|
    |       1| 4561|      967|         21.2|
    |       2|  754|      350|        46.42|
    |       3|  216|      128|        59.26|
    |       4|   70|       38|        54.29|
    |       5|   18|       13|        72.22|
    |       6|    5|        3|         60.0|
    |       7|    1|        0|          0.0|
    +--------+-----+---------+-------------+
    
    

Phân tích cho thấy **tỷ lệ đồng ý tăng rõ rệt** theo **số lần khách hàng từng được liên hệ trong quá khứ**:

- **previous = 0:** 8.83% khách hàng đồng ý.  
- **previous = 1–2:** 21–46%, tăng gấp **2–5 lần**.  
- **previous ≥ 3:** ~55–72%, dù số mẫu ít hơn.

👉 **Kết luận:**  
**Khách hàng đã từng được liên hệ trước đây** là nhóm **có tiềm năng cao**,  
với **xác suất “yes” cao hơn nhiều** so với khách hàng mới.

**Đề xuất:**  
Tập trung **chăm sóc và tái liên hệ nhóm khách hàng quen thuộc**,  
thay vì **dàn trải nguồn lực cho nhóm chưa từng tương tác**.

----

### 3 – Thời lượng và hành vi cuộc gọi (duration)

#### 3.1. Thời lượng và hành vi cuộc gọi – `duration`

→ **Mục tiêu:** phân tích xem **thời lượng cuộc gọi** có liên quan đến **khả năng khách hàng đồng ý (“yes”)** hay không,  
từ đó hiểu rõ **mức độ quan tâm và tương tác** của khách hàng trong quá trình tư vấn.

**Cần phân tích:**

- So sánh **thời lượng trung bình (duration)** giữa hai nhóm **“yes”** và **“no”**.  
- Tính **tỷ lệ “yes” theo nhóm độ dài cuộc gọi**: *ngắn*, *trung bình*, *dài*.

**Câu hỏi khai thác:**

- Liệu **cuộc gọi kéo dài hơn** có thực sự **dẫn đến khả năng đồng ý cao hơn**?  
- Có thể **xác định ngưỡng thời lượng tối thiểu** để nhận biết **cuộc gọi tiềm năng thành công** không?



```python
# Thời lượng trung bình giữa hai nhóm "yes" và "no"
duration_avg = (
    df.groupBy("y")
      .agg(F.round(F.avg("duration"), 2).alias("avg_duration_sec"))
      .orderBy("y")
)
duration_avg.show()

# 2) Tỷ lệ "yes" theo nhóm độ dài cuộc gọi: ngắn (≤120s), trung bình (121–300s), dài (>300s)
duration_bins = (
    df.withColumn(
        "duration_group",
        F.when(F.col("duration") <= 120, "ngắn (≤120s)")
         .when(F.col("duration") <= 300, "trung bình (121–300s)")
         .otherwise("dài (>300s)")
    )
    .groupBy("duration_group")
    .agg(
        F.count("*").alias("total"),
        F.sum((F.col("y") == "yes").cast("int")).alias("yes_count")
    )
    .withColumn("yes_ratio (%)", F.round(F.col("yes_count")/F.col("total")*100, 2))
    .orderBy("duration_group")
)

duration_bins.show()





```

    +---+----------------+
    |  y|avg_duration_sec|
    +---+----------------+
    | no|          220.84|
    |yes|          553.19|
    +---+----------------+
    
    +--------------------+-----+---------+-------------+
    |      duration_group|total|yes_count|yes_ratio (%)|
    +--------------------+-----+---------+-------------+
    |         dài (>300s)|11204|     3122|        27.87|
    |        ngắn (≤120s)|12917|      166|         1.29|
    |trung bình (121–3...|17067|     1352|         7.92|
    +--------------------+-----+---------+-------------+
    
    

**Phân tích cho thấy:**  
Thời lượng cuộc gọi trung bình của nhóm **“yes”** cao gấp hơn **2 lần** so với nhóm **“no”**:

- **Nhóm “no”:** trung bình **220.84 giây**.  
- **Nhóm “yes”:** trung bình **553.19 giây**.

**Phân nhóm độ dài cuộc gọi:**

- **Ngắn (≤120s):** tỷ lệ đồng ý chỉ **1.29%** trên tổng **12,917 cuộc gọi**.  
- **Trung bình (121–300s):** tỷ lệ tăng lên **7.92%** trên **17,067 cuộc gọi**.  
- **Dài (>300s):** tỷ lệ “yes” vọt lên **27.87%** trên **11,204 cuộc gọi**.

👉 **Kết luận:**  
Các cuộc gọi kéo dài **hơn 5 phút** có khả năng thành công **cao gấp 3–5 lần** so với các cuộc gọi ngắn.  
Điều này cho thấy **mức độ tương tác và thời gian tư vấn** là **yếu tố quyết định quan trọng** cho việc thuyết phục khách hàng.

**Đề xuất:**  
Ngân hàng nên **tập trung vào chất lượng cuộc gọi hơn là số lượng**,  
khuyến khích nhân viên **duy trì cuộc trò chuyện lâu hơn** với **những khách hàng tiềm năng**,  
thay vì **kết thúc sớm**.

---

### 4. Tiền xử lý

#### 4.1.a – Chuẩn hóa biến mục tiêu và tạo biến phụ

**Mục tiêu:**

- Chuyển `y` thành dạng nhị phân (`0/1`).
- Tạo biến `has_contact_before = 1` nếu `pdays != 999`, `0` nếu ngược lại (đã từng liên hệ).
- Giới hạn giá trị `campaign ≤ 10` để tránh outlier cực lớn (bạn từng phát hiện có người bị gọi >50 lần).



```python
df = df.withColumn('y_binary', F.when(F.col('y') == 'yes', 1).otherwise(0))

df = df.withColumn('has_contact_before', (F.col('pdays') != 999).cast('int'))

df = df.withColumn('campaign_capped', F.when(F.col('campaign') > 10, 10).otherwise(F.col('campaign')))
```

#### 4.1.b – Mã hóa biến phân loại

**Mục tiêu:**

- Chuyển các biến dạng chữ (string) sang dạng số để mô hình hiểu được.  
- Trong dataset bạn có ba biến phân loại chính:
  - `contact`
  - `month`
  - `day_of_week`



```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

cat_cols = ["contact", "month", "day_of_week"]

# Tạo các bước index và encode cho từng biến
indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_index", handleInvalid="keep") for c in cat_cols]
encoders = [OneHotEncoder(inputCol=f"{c}_index", outputCol=f"{c}_vec") for c in cat_cols]

pipeline = Pipeline(stages=indexers + encoders)
df_encoded = pipeline.fit(df).transform(df)

df_encoded.select("contact", "contact_index", "contact_vec", 
                  "month", "month_index", "month_vec").show(5, truncate=False)
```

    +---------+-------------+-------------+-----+-----------+--------------+
    |contact  |contact_index|contact_vec  |month|month_index|month_vec     |
    +---------+-------------+-------------+-----+-----------+--------------+
    |telephone|1.0          |(2,[1],[1.0])|may  |0.0        |(10,[0],[1.0])|
    |telephone|1.0          |(2,[1],[1.0])|may  |0.0        |(10,[0],[1.0])|
    |telephone|1.0          |(2,[1],[1.0])|may  |0.0        |(10,[0],[1.0])|
    |telephone|1.0          |(2,[1],[1.0])|may  |0.0        |(10,[0],[1.0])|
    |telephone|1.0          |(2,[1],[1.0])|may  |0.0        |(10,[0],[1.0])|
    +---------+-------------+-------------+-----+-----------+--------------+
    only showing top 5 rows
    
    

#### Vì sao dùng StringIndexer + OneHotEncoder

**Giải thích:**

- Các mô hình như **Logistic Regression**, **MLP** cần dữ liệu dạng **vector số** — mỗi cột đại diện cho một đặc trưng riêng.  
- Dữ liệu chỉ có ít giá trị phân loại (`contact`, `month`, `day_of_week`), nên **One-Hot Encoding** không gây tăng chiều dữ liệu quá mức.  
- `StringIndexer` biến chuỗi thành mã số để `OneHotEncoder` hiểu được, giúp pipeline **ổn định và tái sử dụng cho dữ liệu mới**.  
- Nếu chỉ dùng **Label Encoding**, mô hình sẽ hiểu sai rằng các giá trị có **thứ tự**, làm sai ý nghĩa của biến.
---

#### 4.1.c – Chia dữ liệu Train / Validation / Test

**Mục tiêu:**

Tách dữ liệu thành 3 phần để:

- **Train mô hình (70%)**  
- **Validation** để tinh chỉnh tham số (**15%**)  
- **Test** để đánh giá cuối cùng (**15%**)



```python
from pyspark.sql.window import Window

w = Window.partitionBy('y_binary').orderBy(F.rand(seed = 42))
print(w)
df_strat = (
    df_encoded.withColumn('pr', F.percent_rank().over(w))
)

#Chia ty le
train_df = df_strat.filter(F.col('pr') < 0.7).drop('pr')
val_df = df_strat.filter((F.col('pr') >= 0.7) & (F.col('pr') < 0.85)).drop('pr')
test_df = df_strat.filter(F.col('pr') >= 0.85).drop('pr')

# Kiểm tra quy mô & phân bố nhãn
print("Counts:", train_df.count(), val_df.count(), test_df.count())

for name, subset in [("Train", train_df), ("Validation", val_df), ("Test", test_df)]:
    print(f"\n{name} label distribution:")
    total = subset.count()
    subset.groupBy("y_binary").agg(
        F.count('*').alias('count')
    ).withColumn('ratio (%)', F.round(F.col('count')/total * 100, 2)).show()
```

    <pyspark.sql.window.WindowSpec object at 0x000001627647FCA0>
    Counts: 28831 6178 6179
    
    Train label distribution:
    +--------+-----+---------+
    |y_binary|count|ratio (%)|
    +--------+-----+---------+
    |       0|25583|    88.73|
    |       1| 3248|    11.27|
    +--------+-----+---------+
    
    
    Validation label distribution:
    +--------+-----+---------+
    |y_binary|count|ratio (%)|
    +--------+-----+---------+
    |       0| 5482|    88.73|
    |       1|  696|    11.27|
    +--------+-----+---------+
    
    
    Test label distribution:
    +--------+-----+---------+
    |y_binary|count|ratio (%)|
    +--------+-----+---------+
    |       0| 5483|    88.74|
    |       1|  696|    11.26|
    +--------+-----+---------+
    
    

#### Lý do chọn phương pháp chia dữ liệu

**Giải thích:**

- Bài toán sử dụng **Stratified Hold-out (70/15/15)** để đảm bảo tỷ lệ nhãn `yes/no` được giữ ổn định trong từng tập **Train**, **Validation** và **Test**.  
- Phương pháp này phù hợp vì **dữ liệu đủ lớn (~41k mẫu, tỷ lệ yes ≈ 11%)**, giúp mô hình học được xu hướng tổng quát mà không cần lặp nhiều lần như **K-fold**.

**Ưu điểm của Stratified Hold-out:**

- Nhanh, dễ triển khai trong **PySpark**.  
- Giữ đúng phân phối nhãn cho bài toán **mất cân bằng**.  
- Hạn chế được **chi phí tính toán** so với K-fold, vốn phải huấn luyện mô hình nhiều lần.
---


#### 4.1.d – Class weighting (mặc định) + tuỳ chọn undersampling

**Mục tiêu:**

Giảm thiên lệch do `yes ≈ 11%` nhưng không làm mất dữ liệu quan trọng.  
Chiến lược mặc định: **gán trọng số theo lớp** trên tập **train** (validation/test giữ nguyên để đánh giá công bằng).

#### Công thức tổng quát cho trọng số cân bằng

**Công thức:**

$$
w_i = \frac{N}{k \times n_i}
$$

**Trong đó:**

- $w_i$: trọng số của lớp *i*  
- $N$: tổng số mẫu trong tập huấn luyện  
- $n_i$: số mẫu thuộc lớp *i*  
- $k$: số lượng lớp (với bài toán nhị phân, $k = 2$)

→ Khi lớp “yes” chiếm ít, $n_{\text{yes}}$ nhỏ → $w_{\text{yes}}$ lớn hơn $w_{\text{no}}$.  
Nói cách khác, mỗi lỗi dự đoán sai của lớp thiểu số sẽ bị **phạt nặng hơn** trong quá trình tối ưu hàm mất mát,  
giúp mô hình học **cân bằng giữa hai lớp** mà không cần thay đổi dữ liệu gốc.



```python
cnt = {r['y_binary']: r['count'] for r in train_df.groupBy('y_binary').count().collect()}

n_pos = cnt.get(1, 0)
n_neg = cnt.get(0, 0)
n_tot = n_pos + n_neg

# Trọng số cân bằng kiểu "balanced":
# w_pos = N / (2 * N_pos), w_neg = N / (2 * N_neg)
w_pos = float(n_tot) / (2.0 * n_pos)
w_neg = float(n_tot) / (2.0 * n_neg)

# Gán trọng số cho TRAIN; Val/Test không gán để đánh giá trung thực
train_w = train_df.withColumn(
    "class_weight",
    F.when(F.col("y_binary") == 1, F.lit(w_pos)).otherwise(F.lit(w_neg))
)

train_w.groupBy("y_binary").agg(F.round(F.avg("class_weight"),4).alias("avg_w")).orderBy("y_binary").show()

```

    +--------+------+
    |y_binary| avg_w|
    +--------+------+
    |       0|0.5635|
    |       1|4.4383|
    +--------+------+
    
    

#### Giải thích lựa chọn phương pháp class weighting

**Lý do:**

- Tập dữ liệu đủ lớn, **không cần nhân bản mẫu thiểu số**.  
- Nhiều biến phân loại nên **SMOTE không phù hợp**.  
- Các mô hình **Logistic Regression**, **CatBoost** và **MLP** đều hỗ trợ weighting trực tiếp.  
- Giữ nguyên toàn bộ dữ liệu thật, giúp mô hình học **cân bằng và ổn định hơn**.

**Kết luận:**

Sử dụng **class weighting** giúp duy trì toàn bộ dữ liệu gốc, giảm thiên lệch,  
và phù hợp với cả ba mô hình huấn luyện chính của nhóm.

---



#### 4.1.e – Assemble features

**Mục tiêu:**  
Gom toàn bộ các đặc trưng đầu vào (bao gồm cả dữ liệu **số** và **one-hot encoding**) thành **một cột vector duy nhất** có tên `features`, để sẵn sàng đưa vào giai đoạn huấn luyện mô hình.

**Lưu ý:**  
Không bao gồm trường **`duration`** trong quá trình tổng hợp — nhằm **tránh rò rỉ thông tin** liên quan đến kết quả sau khi cuộc gọi đã kết thúc.



```python
from pyspark.ml.feature import VectorAssembler, StandardScaler

# 1️⃣ Gom đặc trưng
feature_cols = ['previous', 'has_contact_before', 'campaign_capped',
                'contact_vec', 'month_vec', 'day_of_week_vec']

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol='features_unscaled'
)

# 2️⃣ Chuẩn hóa đặc trưng
scaler = StandardScaler(
    inputCol='features_unscaled',
    outputCol='features',
    withMean=True,    # trừ trung bình
    withStd=True      # chia độ lệch chuẩn
)

# 3️⃣ Fit-transform trên TRAIN
from pyspark.ml import Pipeline
pipeline_scale = Pipeline(stages=[assembler, scaler])

train_ready = pipeline_scale.fit(train_w).transform(train_w).select('features', 'y_binary', 'class_weight')
val_ready   = pipeline_scale.fit(train_w).transform(val_df).select('features', 'y_binary')
test_ready  = pipeline_scale.fit(train_w).transform(test_df).select('features', 'y_binary')

train_ready.cache()
val_ready.cache()
test_ready.cache()

```




    DataFrame[features: vector, y_binary: int]



### 5 Huấn luyện mô hình 


#### 5.1.a – Cấu hình & Train mô hình (dùng `class_weight`)

**Mục tiêu:**  
Huấn luyện mô hình **baseline đầu tiên** trên tập dữ liệu `train_ready` để làm **mốc so sánh** với các mô hình sau.


```python
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    featuresCol="features",
    labelCol="y_binary",
    weightCol="class_weight",   # sử dụng trọng số từ 4.1d
    predictionCol="prediction",
    probabilityCol="probability",
    rawPredictionCol="rawPrediction",
    family="binomial",
    elasticNetParam=0.0,        # L2
    regParam=0.01,
    maxIter=100,
    standardization=True
)

lr_model = lr.fit(train_ready)


# Dự đoán trên tập validation để chuẩn bị đánh giá ở bước 5.1.b
val_pred = lr_model.transform(val_ready).select("rawPrediction","probability","prediction","y_binary")
val_pred.show(5, truncate=False)

```

    +----------------------------------------+----------------------------------------+----------+--------+
    |rawPrediction                           |probability                             |prediction|y_binary|
    +----------------------------------------+----------------------------------------+----------+--------+
    |[-2.260313747331963,2.260313747331963]  |[0.09446352753522903,0.905536472464771] |1.0       |1       |
    |[0.1530807568359773,-0.1530807568359773]|[0.5381956295230149,0.4618043704769851] |0.0       |1       |
    |[-1.1563470484992502,1.1563470484992502]|[0.23933167899981153,0.7606683210001884]|1.0       |1       |
    |[-0.8614046541319255,0.8614046541319255]|[0.29704595671759115,0.7029540432824088]|1.0       |1       |
    |[-2.0081656447688454,2.0081656447688454]|[0.11834824403131385,0.8816517559686862]|1.0       |1       |
    +----------------------------------------+----------------------------------------+----------+--------+
    only showing top 5 rows
    
    


```python
from pyspark.sql import functions as F
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def evaluation(pred_data):

    cm = (
        pred_data
        .groupBy("y_binary", "prediction")
        .count()
        .orderBy("y_binary", "prediction")
    )

    cm.show()

    # 2) Tính TP, FP, TN, FN và các metric cơ bản
    agg_row = (
        pred_data
        .withColumn("tp", F.when((F.col("prediction")==1) & (F.col("y_binary")==1), 1).otherwise(0))
        .withColumn("fp", F.when((F.col("prediction")==1) & (F.col("y_binary")==0), 1).otherwise(0))
        .withColumn("tn", F.when((F.col("prediction")==0) & (F.col("y_binary")==0), 1).otherwise(0))
        .withColumn("fn", F.when((F.col("prediction")==0) & (F.col("y_binary")==1), 1).otherwise(0))
        .agg(F.sum("tp").alias("tp"),
            F.sum("fp").alias("fp"),
            F.sum("tn").alias("tn"),
            F.sum("fn").alias("fn"),
            F.count("*").alias("n"))
        .collect()[0]
    )

    tp, fp, tn, fn, n = [agg_row[x] for x in ["tp","fp","tn","fn","n"]]

    accuracy  = (tp + tn) / n if n else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall    = tp / (tp + fn) if (tp + fn) else 0.0
    f1        = 2*precision*recall/(precision+recall) if (precision+recall) else 0.0
    pos_rate  = (tp + fn) / n if n else 0.0  # tỷ lệ dương thật (mất cân bằng)

    print(f"Accuracy : {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall   : {recall:.4f}")
    print(f"F1-score : {f1:.4f}")
    print(f"Positive rate (true +): {pos_rate:.4f}")

    # 3) ROC-AUC và PR-AUC (dùng rawPrediction)
    e_roc = BinaryClassificationEvaluator(
        labelCol="y_binary",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    e_pr  = BinaryClassificationEvaluator(
        labelCol="y_binary",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderPR"
    )

    roc_auc = e_roc.evaluate(val_pred)
    pr_auc  = e_pr.evaluate(val_pred)

    print(f"ROC-AUC : {roc_auc:.4f}")
    print(f"PR-AUC  : {pr_auc:.4f}")

evaluation(val_pred)


```

    +--------+----------+-----+
    |y_binary|prediction|count|
    +--------+----------+-----+
    |       0|       0.0| 4790|
    |       0|       1.0|  692|
    |       1|       0.0|  354|
    |       1|       1.0|  342|
    +--------+----------+-----+
    
    Accuracy : 0.8307
    Precision: 0.3308
    Recall   : 0.4914
    F1-score : 0.3954
    Positive rate (true +): 0.1127
    ROC-AUC : 0.7315
    PR-AUC  : 0.3740
    

#### 5.1.b – Tìm siêu tham số và tối ưu ngưỡng dự đoán (threshold) theo F1 trên validation

**Mục tiêu:**  
Mô hình **Logistic Regression** mặc định sử dụng ngưỡng **0.5** để phân loại, tuy nhiên với tập dữ liệu có tỷ lệ lớp “yes” thấp (~11%), ngưỡng này thường **thiếu nhạy** (recall thấp).  
Do đó, cần **quét qua nhiều giá trị ngưỡng** (ví dụ: từ 0.1 đến 0.9), sau đó:

1. Tính **Precision**, **Recall** và **F1-score** cho từng ngưỡng.  
2. Chọn **ngưỡng có F1 cao nhất** làm giá trị tối ưu.


```python
from itertools import product
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.functions import vector_to_array

# Grid nhỏ, thiên về bớt “phạt” để tăng recall
reg_list = [0.0005, 0.001, 0.01, 0.05]
en_list  = [0.0, 0.5, 1.0]     # L2 / mix / L1
mi_list  = [50, 100]

def max_f1_on_validation(model):
    # tính p1
    val_scored = (model.transform(val_ready)
                        .select("y_binary", vector_to_array("probability").getItem(1).alias("p1"))
                        .cache())
    # quét ngưỡng
    best = {"f1": -1}
    for t in [round(0.02 + 0.01*i, 2) for i in range(80)]:  # 0.02..0.81
        pred = val_scored.select("y_binary", (F.col("p1") >= F.lit(t)).cast("int").alias("pred"))
        a = pred.agg(
            F.sum(F.when((F.col("pred")==1) & (F.col("y_binary")==1), 1).otherwise(0)).alias("tp"),
            F.sum(F.when((F.col("pred")==1) & (F.col("y_binary")==0), 1).otherwise(0)).alias("fp"),
            F.sum(F.when((F.col("pred")==0) & (F.col("y_binary")==0), 1).otherwise(0)).alias("tn"),
            F.sum(F.when((F.col("pred")==0) & (F.col("y_binary")==1), 1).otherwise(0)).alias("fn")
        ).first()
        tp, fp, tn, fn = a.tp, a.fp, a.tn, a.fn
        P = tp/(tp+fp) if (tp+fp) else 0.0
        R = tp/(tp+fn) if (tp+fn) else 0.0
        F1 = 2*P*R/(P+R) if (P+R) else 0.0
        if F1 > best["f1"]:
            best = {"thr": t, "f1": F1, "prec": P, "rec": R}
    return best

best = None
for rp, en, mi in product(reg_list, en_list, mi_list):
    lr = LogisticRegression(
        featuresCol="features", labelCol="y_binary", weightCol="class_weight",
        family="binomial", standardization=True,
        regParam=rp, elasticNetParam=en, maxIter=mi
    )
    m = lr.fit(train_ready)
    s = max_f1_on_validation(m)
    if (best is None) or (s["f1"] > best["f1"]):
        best = {"regParam": rp, "elasticNetParam": en, "maxIter": mi, **s, "model": m}

print("Best-by-F1 params:",
      {"regParam":best["regParam"], "elasticNetParam":best["elasticNetParam"],
       "maxIter":best["maxIter"], "thr":best["thr"]})
print(f"VAL @T={best['thr']:.2f} -> F1={best['f1']:.4f} | P={best['prec']:.4f} | R={best['rec']:.4f}")

```

#### 5.1.c – Kết quả trên tập test



```python
from pyspark.ml.classification import LogisticRegression

lr2 = LogisticRegression(
    featuresCol="features",
    labelCol="y_binary",
    weightCol="class_weight",
    predictionCol="prediction",
    probabilityCol="probability",
    rawPredictionCol="rawPrediction",
    family="binomial",
    elasticNetParam=1.0,  
    regParam=0.01,
    maxIter=50,
    threshold=0.51,       # đúng tên tham số
    standardization=True
)


lr2_model = lr2.fit(train_ready)


# Dự đoán trên tập validation để chuẩn bị đánh giá ở bước 5.1.b
test_pred = lr_model.transform(test_ready).select("rawPrediction","probability","prediction","y_binary")
test_pred.show(5, truncate=False)

evaluation(val_pred)

```

    +------------------------------------------+-----------------------------------------+----------+--------+
    |rawPrediction                             |probability                              |prediction|y_binary|
    +------------------------------------------+-----------------------------------------+----------+--------+
    |[-2.2837631534996956,2.2837631534996956]  |[0.09247664682062362,0.9075233531793764] |1.0       |1       |
    |[1.2387586415065526,-1.2387586415065526]  |[0.7753478639442255,0.2246521360557745]  |0.0       |1       |
    |[0.05602751008121537,-0.05602751008121537]|[0.5140032146088256,0.4859967853911744]  |0.0       |1       |
    |[-3.4337992113305296,3.4337992113305296]  |[0.031255691700562804,0.9687443082994371]|1.0       |1       |
    |[-3.8138147904097583,3.8138147904097583]  |[0.021587544912117226,0.9784124550878828]|1.0       |1       |
    +------------------------------------------+-----------------------------------------+----------+--------+
    only showing top 5 rows
    
    +--------+----------+-----+
    |y_binary|prediction|count|
    +--------+----------+-----+
    |       0|       0.0| 4790|
    |       0|       1.0|  692|
    |       1|       0.0|  354|
    |       1|       1.0|  342|
    +--------+----------+-----+
    
    Accuracy : 0.8307
    Precision: 0.3308
    Recall   : 0.4914
    F1-score : 0.3954
    Positive rate (true +): 0.1127
    ROC-AUC : 0.7315
    PR-AUC  : 0.3740
    


    The Kernel crashed while executing code in the current cell or a previous cell. 
    

    Please review the code in the cell(s) to identify a possible cause of the failure. 
    

    Click <a href='https://aka.ms/vscodeJupyterKernelCrash'>here</a> for more info. 
    

    View Jupyter <a href='command:jupyter.viewOutput'>log</a> for further details.

