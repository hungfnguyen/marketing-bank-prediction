* **Demographics**

  * `age`: tuổi.
  * `job`: nghề (vd: admin., services, technician…).
  * `marital`: tình trạng hôn nhân (married/single/divorced).
  * `education`: trình độ (basic.4y, high.school, university…).
  * `default`: có nợ xấu tín dụng không (yes/no/unknown).
  * `housing`: có vay mua nhà không (yes/no/unknown).
  * `loan`: có vay cá nhân không (yes/no/unknown).
* **Liên hệ/chiến dịch**

  * `contact`: kênh liên hệ (cellular/telephone).
  * `month`, `day_of_week`: thời điểm gọi (tháng/ngày).
  * `duration`: **thời lượng cuộc gọi (giây)** ⟶ **chỉ biết sau khi gọi** (xem bẫy dưới).
  * `campaign`: số lần liên hệ trong chiến dịch hiện tại tới **khách hàng này**.
  * `pdays`: số ngày kể từ lần liên hệ trước **trong chiến dịch trước**; **999** nghĩa là **chưa từng liên hệ trước đó**.
  * `previous`: số lần liên hệ **trước** (trong chiến dịch trước).
  * `poutcome`: kết quả của chiến dịch trước (success/failure/nonexistent).
* **Kinh tế vĩ mô** (time series theo giai đoạn 2008–2010)

  * `emp.var.rate` (tăng trưởng việc làm), `cons.price.idx` (CPI), `cons.conf.idx` (niềm tin tiêu dùng), `euribor3m` (lãi suất 3m), `nr.employed` (số người có việc làm).
* **`y`**: mục tiêu (yes/no) – đăng ký **term deposit**.
