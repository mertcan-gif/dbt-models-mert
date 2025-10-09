{{
  config(
    materialized = 'table',tags = ['scm_kpi']
    )
}}
select  k.[Id]
      ,k.[CreatedBy]
      ,k.[CreatedDate]
      ,k.[UpdatedDate]
      ,k.[UpdatedBy]
      ,k.[AktifMi]
      ,k.[SirketId]
      ,k.[ProjeId]
      ,k.[KazaNo]
      ,k.[KazaTarihi]
      ,k.[Lokasyon]
      ,k.[KazaOlayAciklamasi]
      ,k.[YapilanTibbiMudahale]
      ,k.[DetayliArastirmaMi]
      ,k.[GizliMi]
      ,k.[MevcutSonuc]
      ,k.[PotansiyelRisk]
      ,k.[Notlar]
      ,k.[OlaySiniflandirmalariId]
	  ,os.Adi AS olay_siniflandirma
      ,k.[UstKazaId]
      ,k.[CokluMu]
      ,k.[EtkilenenKisiSayisi]
      ,k.[HipoMu]
      ,k.[CiddiMi]
      ,k.[HipoMuOpsiyonel]
      ,k.[SahaIciMi]
      ,k.[IsiEsnasindaMi]
      ,k.[RevizyonNo]
      ,k.[ISGCMuduruYayinladiMi]
      ,k.[KazaBulgulari]
      ,k.[SirketKazaYayiniYapildi]
      ,k.[HoldingKazaYayiniYapildi]
      ,k.[OnbildirimYayiniYapildi]
      ,k.[ArastirmaRaporuYuklendiMi]
      ,k.[KazaDurumu]
      ,k.[YayinNo]
      ,k.[YanginTipi]
      ,k.[IlkOnayciId]
      ,k.[IlkOnaydanGectiMi]
      ,k.[AlanId]
      ,k.[KazaIleIlgiliFirmaId]
	  ,kf.Tanim_Tr as kaza_ile_ilgili_diger_firma_tip
      ,k.[KazaIleIlgiliFirmaDigerText]
      ,k.[KazaYeriId]
	  ,ky.Tanim_Tr AS kaza_yeri_tanim
      ,k.[AltYukleniciId]
	  ,a.Ad as alt_yuklenici_adi
	  ,a.AktifMi as alt_yuklenici_aktif_mi
	  ,kd.Adi as kazazede_ad
	  ,kd.Soyadi as kazazede_soyad
	  ,kd.Meslek as kazazede_meslek
	  ,kd.CalistigiSureYil as calistigi_sure_yil
	  ,kd.CalistigiSureAy as calistigi_sure_ay
	  ,kd.KayipGunSayisi as kazazede_kayip_gun
	  ,kd.KisitliGunSayisi as kazazede_kisitli_gun_sayi
	  ,kd.Cinsiyet as kazazede_cinsiyet
	  ,kd.Milliyet as kazazede_milliyet
	  ,kd.CalistigiFirma as kazazede_calistigi_firma
	  ,kd.Yas as kazazede_yas
	  ,kd.KazazedeTip as kazazede_tip
	  ,kd.CalistigiSureGun as calistigi_sure_gun
	  ,s.Adi as ronesans_sirket_adi
	  ,s.Kodu as ronesans_sirket_kodu
	  ,s.SirketKoordinatoruKullaniciId as sirket_koordinator_kullanıcı_id
	  ,s.AktifMi AS sirket_aktif_mi
	  ,rc.lifnr as vendor_code
    from [PRODAPPSDB].[ISGC].[dbo].[Kazas] k
	LEFT JOIN [PRODAPPSDB].[ISGC].[dbo].[Kazazedes] kd on kd.KazaId = k.Id
	LEFT JOIN [PRODAPPSDB].[ISGC].[dbo].[Sirkets] s on k.SirketId = s.Id
	LEFT JOIN [PRODAPPSDB].[ISGC].[dbo].[AltYuklenicis] a on k.[AltYukleniciId] = a.Id
	left join [PRODAPPSDB].[ISGC].[dbo].[KazaIleIlgiliFirmas] kf on kf.Id = k.[KazaIleIlgiliFirmaId]
	LEFT JOIN [PRODAPPSDB].[ISGC].[dbo].[OlaySiniflandirmalaris] os on os.Id = k.[OlaySiniflandirmalariId]
	left join [PRODAPPSDB].[ISGC].[dbo].[KazaYeris] ky on ky.Id = k.[KazaYeriId]
  LEFT JOIN (SELECT
              *
              FROM  {{ source('stg_dimensions', 'stg__scm_kpi_t_dim_rsafevendorcodes') }} 
              WHERE 1=1
                    AND word_presencte_ratio = '1' ) rc on rc.rsafe_id = k.AltYukleniciId
