# Marketing Data Analysis & Potential Customer Prediction

README nay tom tat cau truc du an va luong lam viec cho nhom.

## 1. Muc tieu
- Khai thac du lieu marketing ngan hang de du doan khach hang tiem nang.
- Phan tich, truc quan hoa, va xay dung mo hinh ML theo cac huong dan cua mon BDML.

## 2. Cau truc thu muc
```
.
├── AGENTS.md              # Quy tac hoat dong cho Codex
├── README.md              # Tai lieu dinh huong (file nay)
├── config/                # Tham so thuc nghiem, duong dan
├── data/
│   ├── bank-additional/   # Du lieu goc hien co (giu nguyen)
│   ├── processed/         # Du lieu da lam sach / feature (tao sau)
│   └── raw/               # Vi tri luu du lieu goc khac (neu can)
├── docs/                  # Tai lieu huong dan, slide, yeu cau tu giang vien
├── notebooks/             # Notebook EDA & mo hinh cua tung thanh vien
├── src/
│   ├── data_prep/         # Script xu ly du lieu
│   ├── evaluation/        # Ham danh gia, metric, threshold
│   ├── features/          # Feature engineering va chon loc
│   ├── models/            # Dinh nghia mo hinh, train, luu
│   └── visualization/     # Ham ve bieu do dung chung
├── reports/
│   ├── proposal/          # Ban de xuat
│   ├── milestone/         # Bao cao tien do giua ky
│   ├── presentation/      # Slide trinh bay
│   └── final_report/      # Bao cao cuoi ky
├── artifacts/             # Ket qua chay mo hinh, trong so, bieu do xuat
└── logs/                  # Log chay job, seed, thong tin theo doi
```

Ghi chu:
- Khong xoa `data/bank-additional/` cho den khi cap nhat code doc du lieu.
- Khi them thu muc con, dam bao tong so dong moi file < 300.

## 3. Phan cong chinh
- Hung: `notebooks/01_personal_profiles.ipynb`, tap trung dac diem khach hang va Logistic Regression.
- Don: `notebooks/02_contact_channel.ipynb`, phan tich kenh va lich goi, mo hinh CatBoost/Logistic Regression.
- Hai: `notebooks/03_frequency_callbacks.ipynb`, tan suat lien he, Random Forest/GBT PySpark.
- Hao: `notebooks/04_economic_context.ipynb`, bien kinh te va tong hop, LightGBM/CatBoost/MLP.
- Tat ca dong gop `notebooks/10_model_baselines.ipynb` va thu muc `src/`.

## 4. Nguyen tac lam viec
- Du lieu goc chi doc: luon ghi ra `data/processed/` khi lam sach/tao feature.
- Dung chung cac ham trong `src/`, tranh lap code trong notebook.
- Mo hinh va metric luu vao `artifacts/` voi ten file ro rang (`model_<ten thanh vien>_<ngay>.pkl`).
- Giu log train quan trong trong `logs/` de so sanh va tai hien.
- Commit tren nhanh rieng, tao PR gop y truoc khi merge vao main.

## 5. Tai lieu bat buoc
- `docs/BDMLProjectGuides.pdf` + `docs/BDMLReportGuides.pdf` + `docs/BDMLPresentationGuides.pdf` de doi chieu.
- `docs/marketing_analysis_slides.html` lam proposal va follow phan cong.
- Cap nhat them tai lieu bo sung (neu co) vao `docs/` hoac `reports/`.

## 6. Buoc tiep theo
1. Di chuyen/ghi chu du lieu moi vao `data/raw/` va cap nhat duong dan doc trong notebook.
2. Tao cac notebook theo phan cong va day ket qua (EDA, insight).
3. Truu xuat ham chung sang `src/`, dat test don gian neu can.
4. Chuan hoa bao cao trong `reports/` theo cac moc thoi gian Giang vien yeu cau.

Cam on moi nguoi tuan thu cau truc nay de lam viec hieu qua.
