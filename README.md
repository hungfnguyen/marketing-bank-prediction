# Marketing Data Analysis & Potential Customer Prediction

## 1) Muc tieu

* Khai thac du lieu marketing ngan hang de du doan khach hang tiem nang (y: yes/no).
* Phan tich EDA + xay dung mo hinh ML theo huong dan mon BDML.

## 2) Cau truc thu muc (toi thieu)

```
.
├─ README.md
├─ data/
│  └─ bank-additional/   # du lieu goc (sep=';')
├─ docs/                 # tai lieu, slide
├─ notebooks/            # EDA & model theo phan cong
│  ├─ 01_personal_profiles.ipynb
│  ├─ 02_contact_channel.ipynb
│  ├─ 03_frequency_callbacks.ipynb
│  ├─ 04_economic_context.ipynb
├─ artifacts/            # (tuy chon) luu metric/anh/model
└─ logs/                 # (tuy chon) log train

```

## 3) Phan cong

* **Hung**: 01_personal_profiles.ipynb (Logistic; insight theo age/job/education, marital, housing/loan/default).
* **Don**: 02_contact_channel.ipynb (CatBoost/LogReg; kenh & lich goi).
* **Hai**: 03_frequency_callbacks.ipynb (RF/GBT PySpark; so lan goi, callback).
* **Hao**: 04_economic_context.ipynb (LightGBM/CatBoost/MLP; bien vi mo).

## 5) Tai lieu

* `docs/BDMLProjectGuides.pdf`, `docs/BDMLReportGuides.pdf`, `docs/BDMLPresentationGuides.pdf`
* `docs/marketing_analysis_slides.html`
