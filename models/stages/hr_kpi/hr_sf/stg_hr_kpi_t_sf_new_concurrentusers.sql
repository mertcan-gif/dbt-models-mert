{{
  config(
    materialized = 'table',tags = ['sf_new_api']
    )
}}


/** HR ALL'dan filtrelenmesi gereken Concurrent kullanıcıların user_id'leri

Filtreleme mantığı:
    Person ID'leri aynı olan, User ID'leri farklı olan kullanıcılardan Aktif statüye sahip kullanıcıların alınması, Pasiflerin filtrelenmesi
    Person ID'leri aynı olan, User ID'leri farklı olan kullanıcıların iki kaydının da statüsü Pasif ise, en güncel olan Pasif kaydın alınıp eski kaydın filtrelenmesi

**/

	SELECT
		user_id
	FROM (
		SELECT 
			c.user_id
			,ROW_NUMBER() OVER(PARTITION BY c.person_id ORDER BY c.job_end_date ASC) RN  
		FROM {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} c
			JOIN {{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} d ON c.person_id = d.person_id
		WHERE 1=1
			AND c.user_id <> d.user_id
			/** SAC Reporttaki diğer tüm kayıtlardan, concurrent kullanıcıların pasif kayıtlarını çıkartıyoruz.
				Bu sebeple SAC Reportta tutmak istemediğimiz user_id'lerin olduğu bir CTE oluşturuyoruz 
				Bu sebeple aktif kayıtları aşağıda filtreliyoruz, sadece istemediğimiz user_id'leri elde etmek için **/
			AND c.user_id NOT IN ( 
									/** Concurrent kişilerin aktif olan kayıtları bulunur **/
									SELECT 
										a.user_id
									FROM 
										{{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} a
									JOIN 
										{{ source('stg_sf_odata', 'raw__hr_kpi_t_sf_newsf_employees') }} b ON a.person_id = b.person_id
									WHERE 1=1
										and a.user_id <> b.user_id
										and a.employee_status_en = 'Active'
									)
		) concurrent_filter
		/** Concurrent kullanıcıların iki kaydının da T olması durumunda, SAC Report'tan çıkartacağımız kayıt eski kayıt olacaktır
		Bu sebeple Row Number kullanarak kullanacağımız kayıtları buradan eliyoruz. **/
	WHERE RN = 1

