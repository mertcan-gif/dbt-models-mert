import requests
import pandas as pd
import json
import pyodata
import sqlalchemy
import yaml
import ronesans_helper.config as cfg



aws_dev_connectionstring = cfg.dict_all_server['SQL']['AWS_STAGE']['Constring']

class NonAppServerConnector:
    """
    Verilen bağlantı dizesini kullanarak bir SQLAlchemy bağlantı motoru oluşturur.

    Attributes:
        connection_engine: SQLAlchemy bağlantı motoru nesnesi.

    Args:
        connection_string (str): Veritabanı bağlantı dizesi.
    """
    
    def __init__(self, connection_string:str):
        """
        NonAppServerConnector sınıfını başlatır.

        Args:
            connection_string (str): Veritabanı bağlantı dizesi.
        """
        self.connection_engine = sqlalchemy.create_engine(
            connection_string, 
            fast_executemany=True, 
            connect_args={'connect_timeout': 10}, 
            echo=False
        )

    def close_connection(self):
        """
        Bağlantıyı kapatır.
        """
        self.connection_engine.dispose()  # connection_engine üzerinden bağlantıyı kapatma


def sqlcol(dfparam: pd.DataFrame) -> dict:
    """
    Bir pandas DataFrame'deki sütunlara SQLAlchemy veri türlerini eşleyen bir sözlük döndürür.

    Args:
        dfparam (pd.DataFrame): SQL tablo yapısına dönüştürülecek pandas DataFrame.

    Returns:
        dict: DataFrame sütunlarının SQLAlchemy veri türlerine eşlenmiş hali.
    """
    dtypedict = {}
    for i, j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length='max')})
        if "datetime64[ns, UTC]" in str(j):
            dtypedict.update({i: sqlalchemy.types.DATETIME()})
    return dtypedict


def load_to_sql(connection_string: str, schema: str, sql_table_name: str, dataframe: pd.DataFrame) -> None:
    """
    Bir pandas DataFrame'i, tabloyu veritabanına yüklemeden önce ilgili tabloyu 'drop' ederek, veritabanına yükler.

    Args:
        connection_string (str): Veritabanı bağlantı dizesi.
        schema (str): DataFrame'in yükleneceği şema ismi.
        sql_table_name (str): DataFrame'in yükleneceği tablo ismi.
        dataframe (pd.DataFrame): Veritabanına yüklenecek tabloyu içeren pandas DataFrame.
    """
    nonappserver_connector = NonAppServerConnector(connection_string)
    outputdict = sqlcol(dataframe)
    dataframe.to_sql(
        sql_table_name, 
        nonappserver_connector.connection_engine, 
        schema=schema, 
        dtype=outputdict, 
        index=False, 
        if_exists='replace'
    )
    del nonappserver_connector  # Bağlantı nesnesini temizle

def get_odata_entity_results(
    client: pyodata.Client,
    entity_set_name: str,
    select_parameters: str = '',
    filter_parameters: str = '',
    expand_parameters: str = '',
    from_date_parameter: str = ''
):
    """
    Bir pyOdata client'ını parametre olarak alır daha sonra, bu client ile birlikte parametre olarak aldığı entityset'e bir request gönderir.
    Bu requestte aynı zamanda, select, filter, expand, ve from_date parametrelerini de kullanır.
    Requeste gelen cevapta next_url bilgisi bir sonraki veri setini içerir, bu sayede son next_url gelmeyene kadar, yani pagination bitene kadar
    tüm veriler çekilir, entity_results extend edilerek üzerine eklenir ve bütün veriye erişim sağlanır.

    Args:
        client (pyodata.Client): OData istemcisi.
        entity_set_name (str): Verilerin alınacağı OData varlık kümesinin adı.
        select_parameters (str, optional): Alınacak sütunları belirten bir dize. Varsayılan boş dizedir.
        filter_parameters (str, optional): Veri filtreleme kriterlerini belirten bir dize. Varsayılan boş dizedir.
        expand_parameters (str, optional): İlişkili varlıkları genişletmek için kullanılan bir dize. Varsayılan boş dizedir.
        from_date_parameter (str, optional): Verilerin alınacağı başlangıç tarihini belirten bir dize. Varsayılan boş dizedir.

    Returns:
        EntityResults: İlgili OData varlık kümesinden alınan veriler.

    Notes:
        - `entity_results_query` oluşturulurken `eval` kullanımı güvenlik riskleri taşıyabilir. Mümkünse, `eval`'in kullanımından kaçınılması önerilir.
        - Fonksiyon, `next_url` bilgisi olan verileri alana kadar sayfalama yapar ve tüm sonuçları birleştirir.
    """
    entity_results_query = f"client.entity_sets.{entity_set_name}.get_entities()"
    
    if select_parameters:
        entity_results_query += f'.select("{select_parameters}")'
        
    if filter_parameters:
        entity_results_query += f'.filter("{filter_parameters}")'
        
    if expand_parameters:
        entity_results_query += f'.expand("{expand_parameters}")'
        
    if from_date_parameter:
        entity_results_query += f'.custom("fromDate", "{from_date_parameter}")'
    
    # Dinamik sorgu çalıştırma
    # print(entity_results_query)
    entity_results = eval(f"{entity_results_query}.execute()")
    
    is_first_run = True
    x = 0
    while True:
        x += 1
        if entity_results.next_url:
            if is_first_run:
                entity_results_iterated = eval(
                    f"client.entity_sets.{entity_set_name}.get_entities().next_url(entity_results.next_url).execute()"
                )
                is_first_run = False
            else:
                entity_results_iterated = eval(
                    f"client.entity_sets.{entity_set_name}.get_entities().next_url(entity_results_iterated.next_url).execute()"
                )
            entity_results.extend(entity_results_iterated)
            if entity_results_iterated.next_url is None:
                break
        else:
            break
    
    return entity_results


def pick_list_finder(picklist_value,pick_list_language) -> str:
    """
    SfOdatada verilerin bir çoğu picklist üzerinden gelmektedir, burada verilerin türkçesi ingilizcesi gibi bilgileri yer almaktadır.
    Biz default olarak Türkçesini döndürmekteyiz.
    Verilen bir picklist (seçim listesi) içindeki öğelerden, Türkçe (tr_TR) olanının etiketini bulur.

    Args:
        picklist_value (Iterable): Her bir öğesi 'locale' ve 'label' özelliklerine sahip olan bir iterable (genellikle bir liste).

    Returns:
        str: Türkçe (tr_TR) olan öğenin etiketini döndürür. Eğer Türkçe öğe bulunamazsa, None döner.

    Notes:
        - `picklist_value` iterable (liste, tuple vb.) olmalıdır ve her bir öğe `locale` ve `label` niteliklerine sahip olmalıdır.
    """
    for item in picklist_value:
        if item.locale == pick_list_language:
            return item.label
    return None



def get_config(file_path: str, table: str) -> dict:
    """
    Verilen dosya yolundan YAML yapılandırma dosyasını okuyarak belirtilen tablo için konfigürasyon bilgilerini döndürür.

    Args:
        file_path (str): YAML yapılandırma dosyasının tam dosya yolu.
        table (str): Konfigürasyon bilgilerini almak için kullanılan tablo adı.

    Returns:
        dict: Tablo adı ile ilişkili konfigürasyon bilgilerini içeren bir sözlük. 
              İçerdiği anahtarlar: 'entity_set_name', 'select_parameters', 'expand_parameters',
              'filter_parameters', 'from_date_parameter', ve 'columns'.

    Notes:
        - `yaml` modülünün `safe_load` fonksiyonu kullanılarak dosya içeriği güvenli bir şekilde yüklenir.
        - 'from_date_parameter' anahtarı opsiyonel olup, dosyada mevcut değilse `None` olarak ayarlanır.
    """
    config = dict()
    
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
        
    config['entity_set_name'] = data[table].get('odata_entity', '')
    config['select_parameters'] = data[table].get('select_parameters', '')
    config['expand_parameters'] = data[table].get('expand_parameters', '')
    config['filter_parameters'] = data[table].get('filter_parameters', '')
    config['from_date_parameter'] = data[table].get('from_date_parameter', None)
    config['columns'] = data[table].get('columns', [])
    
    return config


def recursive_iteration(_final_value,_keys,picklist_language = None):
    """
    Verilen anahtarlar listesine göre `_final_value` üzerinde özyinelemeli (recursive) olarak iterasyon yapar.

    Args:
        _final_value (Any): Üzerinde iterasyon yapılacak başlangıç değeri. Bu değer, nesneler, listeler veya diğer veri türleri olabilir.
        _keys (Iterable[str]): `_final_value` üzerinde erişim sağlamak için kullanılacak anahtarlar listesi. Her anahtar, `_final_value`'ın bir özelliği veya liste indeksini belirtir.

    Returns:
        Any: Belirtilen anahtarlar listesi kullanılarak elde edilen sonuç. Eğer anahtarlar listesi `_final_value`'ı `None` yaparsa, `None` döner.

    Notes:
        - `pick_list_finder` fonksiyonu, eğer anahtar `picklistLabels` ise çağrılır ve özel bir işlev yapar.
        - `_final_value` bir liste olduğunda, sadece ilk elemanla devam edilir. Eğer liste boşsa, `_final_value` `None` olarak ayarlanır.
    """
    for key in _keys:
        _final_value = getattr(_final_value, key)
        if key == 'picklistLabels':
            _final_value = pick_list_finder(_final_value,picklist_language)    
        if type(_final_value) == list:
            if len(_final_value) == 0:
                _final_value = None
                break
            _final_value = _final_value[0]
        if _final_value is None:
            break
    return _final_value

def turn_odata_entity_to_dataframe(entity_results,column_configurations):
    """
    OData varlık sonuçlarını ve sütun yapılandırmalarını kullanarak bir pandas DataFrame oluşturur.

    Args:
        entity_results (list): OData varlık sonuçlarını içeren bir liste. Her bir öğe, bir varlık sonucunu temsil eden bir nesne olmalıdır.
        column_configurations (dict): Her bir anahtarın (sütun adı) OData varlık sonucundaki değerini belirten konfigürasyonlar içeren bir sözlük.
            Anahtarlar, OData varlık sonucunda yol belirtir ve değerler bu yola karşılık gelen sütun adını temsil eder.

    Returns:
        pd.DataFrame: OData varlık sonuçlarına dayalı olarak oluşturulmuş bir pandas DataFrame.

    Notes:
        - `recursive_iteration` fonksiyonu, her bir varlık sonucundaki verileri çekmek için kullanılır.
        - `column_configurations` sözlüğü, verilerin nasıl çıkarılacağını belirten anahtar/değer çiftlerinden oluşur.
    """
    final_dataframe = []
    for entity_result in entity_results:
        _temp_dict = dict()
        for _key,_value in column_configurations.items():
            if "picklistLabels" in _key:
                _keys = _key.split("/")
                _final_value = entity_result
                ## tr_TR
                _final_value_tr = recursive_iteration(_final_value,_keys,"tr_TR")
                _temp_dict[_key+"_tr"] = _final_value_tr
                ## en_US
                _final_value_en = recursive_iteration(_final_value,_keys,"en_US")
                _temp_dict[_key+"_en"] = _final_value_en
            else:
                _keys = _key.split("/")
                _final_value = entity_result
                _final_value = recursive_iteration(_final_value,_keys)
                _temp_dict[_key] = _final_value
        final_dataframe.append(_temp_dict)
    
    df = pd.DataFrame(final_dataframe)
    return df

def rename_columns(df,dict_of_raw_columns):
    """
    Pandas DataFrame'deki sütun isimlerini, verilen sözlük kullanılarak yeniden adlandırır.

    Args:
        df (pd.DataFrame): Sütun isimleri yeniden adlandırılacak pandas DataFrame.
        dict_of_raw_columns (dict): Eski sütun isimlerini yeni isimlerle eşleyen bir sözlük.
            Anahtarlar eski sütun isimlerini, değerler ise yeni sütun isimlerini temsil eder.

    Returns:
        pd.DataFrame: Sütun isimleri yeniden adlandırılmış pandas DataFrame.

    Notes:
        - `dict_of_raw_columns` sözlüğündeki her anahtar, DataFrame'deki mevcut bir sütun adı olmalıdır.
        - Sözlükteki her değer, eski sütun adının yerine geçecek yeni sütun adıdır.
    """
    rename_dict = dict()
    for key,value in dict_of_raw_columns.items():
        if "picklistLabels" in key:
            rename_dict[key+"_tr"] = dict_of_raw_columns[key]+"_tr"
            rename_dict[key+"_en"] = dict_of_raw_columns[key]+"_en"
        else:
            rename_dict[key] = dict_of_raw_columns[key]
    return df.rename(columns=rename_dict)


def get_data_from_odata(username,password,table,file_path,service_url):
    """
    OData hizmetinden veri alır, yapılandırma dosyasını kullanarak verileri çeker ve bir pandas DataFrame oluşturur.

    Args:
        username (str): OData hizmetine erişim için kullanılan kullanıcı adı.
        password (str): OData hizmetine erişim için kullanılan parola.
        table (str): Yapılandırma dosyasında bulunan tablo adı.
        config_file (str): Yapılandırma dosyasının adı (yaml formatında), genellikle tablo adı ile ilişkilidir.

    Returns:
        pd.DataFrame: OData hizmetinden alınan verilerle oluşturulmuş pandas DataFrame.

    Notes:
        - `get_config` fonksiyonu yapılandırma dosyasından gerekli ayarları alır.
        - `get_odata_entity_results` fonksiyonu OData varlık sonuçlarını alır.
        - `turn_odata_entity_to_dataframe` fonksiyonu OData varlık sonuçlarını DataFrame'e dönüştürür.
        - `rename_columns` fonksiyonu DataFrame'deki sütun adlarını yeniden adlandırır.
        - Tüm sütunlar 'object' türünden 'str' türüne dönüştürülür.
    """
    auth_info = (username,password)
    session = requests.Session()
    session.auth = auth_info
    _client = pyodata.Client(service_url, session)


    ############ CREATE CONFIG ############ 
    _table = table
    # file_path = rf'\\RNS-PRODSQL01\Prefect_Instance\sfodata-configs\{str.upper(config_file)}_pyodata_config.yaml'
    _file_path = file_path
    config = get_config(_file_path,_table)

    ############ CREATE PYODATA ############ 
    entity_instance = get_odata_entity_results(_client,config['entity_set_name'],
                                    config['select_parameters'],
                                    config['filter_parameters'],
                                    config['expand_parameters'],
                                    config['from_date_parameter']
                                    )

    ############ CREATE DATAFRAME ############ 
    df = turn_odata_entity_to_dataframe(entity_instance,config['columns'])
    df= rename_columns(df,config['columns'])
    ############ RENAME DATAFRAME ############ 
    df = df.astype({col: 'str' for col in df.select_dtypes(include=['object']).columns})
    return df