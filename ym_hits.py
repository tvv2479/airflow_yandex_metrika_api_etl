from airflow.decorators import dag, task
# from airflow.sdk.dag import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, date 
from time import sleep
import logging
import pendulum
import json
import pandas as pd
from io import StringIO

# ==== Настройки ====

CONN_TOKEN = "ym_token_logs"  # ID HTTP-соединения в Airflow
COUNTER = 65439712
BASE_URL = f'https://api-metrika.yandex.net/management/v1/counter/{COUNTER}/'

date_start = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
date_end = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")

# Создаем подключение к базе
hook = PostgresHook(postgres_conn_id='staging_db')
engine = hook.get_sqlalchemy_engine()

# Параметы hits

source_hits = 'hits'

fields_hits = (
    'ym:pv:watchID', 'ym:pv:pageViewID', 'ym:pv:counterID', 'ym:pv:clientID', 'ym:pv:counterUserIDHash', 'ym:pv:date',
    'ym:pv:dateTime', 'ym:pv:title', 'ym:pv:pageCharset', 'ym:pv:goalsID', 'ym:pv:URL', 'ym:pv:referer', 'ym:pv:UTMCampaign',
    'ym:pv:UTMContent', 'ym:pv:UTMMedium', 'ym:pv:UTMSource', 'ym:pv:UTMTerm', 'ym:pv:openstatAd', 'ym:pv:openstatCampaign',
    'ym:pv:openstatService', 'ym:pv:openstatSource', 'ym:pv:operatingSystem', 'ym:pv:from', 'ym:pv:hasGCLID', 'ym:pv:GCLID',
    'ym:pv:lastTrafficSource', 'ym:pv:lastSearchEngineRoot', 'ym:pv:lastSearchEngine', 'ym:pv:lastAdvEngine',
    'ym:pv:lastSocialNetwork', 'ym:pv:lastSocialNetworkProfile', 'ym:pv:recommendationSystem', 'ym:pv:messenger', 'ym:pv:browser',
    'ym:pv:browserMajorVersion', 'ym:pv:browserMinorVersion', 'ym:pv:browserCountry', 'ym:pv:browserEngine',
    'ym:pv:browserEngineVersion1', 'ym:pv:browserEngineVersion2', 'ym:pv:browserEngineVersion3', 'ym:pv:browserEngineVersion4',
    'ym:pv:browserLanguage', 'ym:pv:clientTimeZone', 'ym:pv:cookieEnabled', 'ym:pv:deviceCategory', 'ym:pv:javascriptEnabled',
    'ym:pv:mobilePhone', 'ym:pv:mobilePhoneModel', 'ym:pv:operatingSystemRoot', 'ym:pv:physicalScreenHeight',
    'ym:pv:physicalScreenWidth', 'ym:pv:screenColors', 'ym:pv:screenFormat', 'ym:pv:screenHeight', 'ym:pv:screenOrientation',
    'ym:pv:screenOrientationName', 'ym:pv:screenWidth', 'ym:pv:windowClientHeight', 'ym:pv:windowClientWidth', 'ym:pv:ipAddress',
    'ym:pv:regionCity', 'ym:pv:regionCountry', 'ym:pv:regionCityID', 'ym:pv:regionCountryID', 'ym:pv:isPageView', 'ym:pv:isTurboPage',
    'ym:pv:isTurboApp', 'ym:pv:iFrame', 'ym:pv:link', 'ym:pv:download', 'ym:pv:notBounce', 'ym:pv:artificial', 'ym:pv:ecommerce',
    'ym:pv:params', 'ym:pv:parsedParamsKey1', 'ym:pv:parsedParamsKey2', 'ym:pv:parsedParamsKey3', 'ym:pv:parsedParamsKey4',
    'ym:pv:parsedParamsKey5', 'ym:pv:parsedParamsKey6', 'ym:pv:parsedParamsKey7', 'ym:pv:parsedParamsKey8', 'ym:pv:parsedParamsKey9',
    'ym:pv:parsedParamsKey10', 'ym:pv:httpError', 'ym:pv:networkType', 'ym:pv:shareService', 'ym:pv:shareURL', 'ym:pv:shareTitle'
)

params_hits = {'date1': date_start,
               'date2': date_end,
               'source': source_hits,
               'fields': fields_hits}

# ==== DAG ====

@dag(
    dag_id="hits_logs_ym",
    start_date=datetime(2025, 1, 1),
    schedule="15 2 * * *", # Запуск в 07:15 утра по НСО каждый день
    catchup=False,
    tags=["yandex_metrica", "logs", "vitaliy"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
)
def ym_hits_dag():
    def clean_log_request(request_id):
        '''Функция для очистки журнала логов.'''
        
        # Получаем токен из Airflow Connection
        http_hook = HttpHook(method='POST', http_conn_id=CONN_TOKEN)
        metrica_token = http_hook.get_connection(CONN_TOKEN).password
            
        logging.info(f"Step_4_hits. Ochistka jurnala dlia zaprosa #{request_id}.")
        # Формируем endpoint (Airflow НЕ принимает полный URL!)
        endpoint = f"management/v1/counter/{COUNTER}/logrequest/{request_id}/clean"
        
        # Формируем хедеры с токеном
        headers_post = {
            "Authorization": f"OAuth {metrica_token}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        try:
            response = http_hook.run(
                endpoint=endpoint,
                headers=headers_post,
                extra_options={'check_response': False}
            )
            
            while response.status_code != 200:
                logging.warning(f"Step_4_hits. Status code {response.status_code} pri ochistke jurnala #{request_id}. Povtornaia popitka.")
                sleep(5)
                response = http_hook.run(
                    endpoint=endpoint,
                    headers=headers_post,
                    extra_options={'check_response': False}
                )

            logging.info(f"Step_4_hits. Uspeshnaia ochistka jurnala # {request_id}.")
        except Exception as err:
            logging.error(f"Step_4_hits. Oshibka pri ochistke jurnala #{request_id}: {err}", exc_info=True)
            
    @task(show_return_value_in_logs=False) # Отключает вывод в системные логи всех загруженных данных ЯндексМетрики
    def request_logs_ym(params_hits):
        
        # ======= ШАГ 1. Создание лога =======
        
        logging.info("Step_1_hits. Sozdanie loga.")
        try:
            max_attempts = 5
            current_attempt = 0
    
            # 1) Получаем токен из Airflow Connection
            http_hook = HttpHook(method='POST', http_conn_id=CONN_TOKEN)
            metrica_token = http_hook.get_connection(CONN_TOKEN).password
            
            # 2) Формируем хедеры с токеном
            headers_post = {
                "Authorization": f"OAuth {metrica_token}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            # 3. Формируем endpoint (Airflow НЕ принимает полный URL!)
            endpoint = f"management/v1/counter/{COUNTER}/logrequests.json"
            
            # 4. POST-запрос через HttpHook
            response = http_hook.run(
                endpoint=endpoint,
                headers=headers_post,
                data=params_hits    #json.dumps(params_hits)  # тело POST запроса
            )
            
            # Запускаем цикл: пока код ответа не 200
            # И количество попыток меньше max_attempts
            while response.status_code != 200 and current_attempt < max_attempts:
                logging.warning(f"Step_1_hits. Status code {r.status_code}. Popitka {current_attempt + 1} iz {max_attempts}.")
                sleep(5)
                response = http_hook.run(endpoint=endpoint, headers=headers_post, data=params_hits)
                current_attempt += 1
                
            if response.status_code != 200:
                logging.error(f"Step_1_hits. Ne udalos sozdat log posle {max_attempts} popitok. Status code: {response.status_code}. Otvet: {response.text}")
                pass
            
            # Распаковка ответа
            data1 = response.json()
            
            requestId = data1['log_request'].get('request_id')
            status = data1['log_request'].get('status')
            

            # return requestId
            logging.info(f"Результат задачи: {requestId}")
            logging.info(f"Step_1_hits. Uspeshnoe sozdanie loga. Request number {requestId}. Request status {status}.")
            
        except Exception as err:
            logging.error(f"Step_1_hits. Proizoshla oshibka pri sozdanii loga: {err}", exc_info=True)
            pass
        
        # ======= ШАГ 2. Ожидание обработки =======

        http_hook_get = HttpHook(method='GET', http_conn_id=CONN_TOKEN)
        
        headers_get = {'Authorization': f"OAuth {metrica_token}",
                       'Accept': 'application/json'}
        
        # Формируем endpoint (Airflow НЕ принимает полный URL!)
        endpoint = f"management/v1/counter/{COUNTER}/logrequest/{requestId}"
        timeout_seconds = 900 # 15 минут таймаут
        
        sleep(10)
        
        # Для выгрузки логов статус обработки должен быть: processed
        logging.info(f"Step_2_hits. Ожидание обработки лога #{requestId}.")

        try:
            start_time = datetime.now()

            # Делаем проверку с обработкой 404 как временного состояния
            # Если 404, просто ждем дальше в цикле while
            response = http_hook_get.run(
                endpoint=endpoint, 
                headers=headers_get, 
                extra_options={'check_response': False}
                )

            while response.status_code != 200 or response.json().get('log_request', {}).get('status') != 'processed':
                if (datetime.now() - start_time).total_seconds() > timeout_seconds:
                    logging.error(f"Step_2_hits. Превышен таймаут ожидания обработки лога #{requestId}.")
                    # Попытка очистить журнал перед выходом
                    #clean_log_request(requestId)
                    #None

                if response.status_code != 200:
                        logging.warning(f"Step_2_hits. Получен статус {response.status_code} при проверке статуса лога #{requestId}. Повторная попытка.")
                        logging.warning(f"Step_2_hits. URL: {response}")
                elif response.json().get('log_request', {}).get('status') != 'processed':
                        logging.info(f"Step_2_hits. Статус лога #{requestId}: {response.json().get('log_request', {}).get('status')}. Ожидание.")

                sleep(5)

                response = http_hook_get.run(
                endpoint=endpoint, 
                headers=headers_get, 
                extra_options={'check_response': False}
                )

                data2 = response.json()

            if 'log_request' not in data2 or 'parts' not in data2.get('log_request', {}):
                logging.error(f"Step_2_hits. В ответе API отсутствует ключ 'log_request' или 'parts'. Ответ: {data2}")
                # Попытка очистить журнал перед выходом
                #clean_log_request(requestId)
                #None

            parts = data2['log_request']['parts']
            status = data2['log_request']['status']

            logging.info(f"Step_2_hits. Лог #{requestId} обработан. Количество частей - {len(parts)}. Request status {status}.")
        except Exception as err:
            logging.error(f"Step_2_hits. Произошла ошибка при проверке статуса лога #{requestId}: {err}", exc_info=True)
            None

            
        # === ШАГ 3. Загрузка данных ===
        logging.info("Step_3: загрузка логов")

        http_hook_download = HttpHook(method='GET', http_conn_id=CONN_TOKEN)

        res = []

        try:
            if len(parts) == 1:
                # Если всего одна часть выгрузки
                endpoint = f"management/v1/counter/{COUNTER}/logrequest/{requestId}/part/0/download"
                resp = http_hook_download.run(
                    endpoint=endpoint, 
                    headers=headers_get, 
                    extra_options={'check_response': False}
                    )
                
                while resp.status_code != 200:
                    logging.warning(f"Step_3_hits. Получен статус {resp.status_code} при загрузке части 1 лога #{requestId}. Повторная попытка.")
                    sleep(5)

                    resp = http_hook_download.run(
                        endpoint=endpoint, 
                        headers=headers_get, 
                        extra_options={'check_response': False}
                        )

                res = resp.text
                logging.info("Step_3_hits. Zagruzka logov iz 1 chasti.")
                logging.info(f"Тип переменной: {type(res)}")
            # Если частей выгрузки болльше чем одна
            else:
                a = list(range(0, len(parts)))
                d = []
                for i in a:
                    endpoint = f"management/v1/counter/{COUNTER}/logrequest/{requestId}/part/{i}/download"
                    resp = http_hook_download.run(
                        endpoint=endpoint, 
                        headers=headers_get, 
                        extra_options={'check_response': False}
                        )

                    while resp.status_code != 200:
                        sleep(5)
                        resp = http_hook_download.run(
                            endpoint=endpoint, 
                            headers=headers_get, 
                            extra_options={'check_response': False}
                            )
            
                    d.append(resp.text)

                res.append(d)
                logging.info(f"Step_3_hits. Zagruzka {len(parts)} chastei logov.")
                logging.info(f"Тип переменной: {type(res)}")

        except Exception as err:
            logging.error(f"Step_3_hits. Oshibka zagtuzki logov: {err}", exc_info=True)
            None
        
        clean_log_request(requestId)

        return res

    @task
    def logs_to_db(logs_ym, engine):
        logging.info(f"Получено логов {len(logs_ym[0])}")
        
        # ===== ЗАГРУЗКА ДАННЫХ В БАЗУ =====

        try:
            logging.info("Chtenie dannih hits i zapis v dataframe.")
            # Пеобразование логов в DataFrame
            if isinstance(logs_ym, str):
                df_hit = pd.read_csv(StringIO(logs_ym), sep='\t', low_memory=False)
                mask_header_row = (df_hit == df_hit.columns).all(axis=1)
                df_hit = df_hit[~mask_header_row]
            elif isinstance(logs_ym, list):    
                # 1. "Сплющиваем" список списков в один плоский список строк
                flat_list = [item for sublist in logs_ym for item in sublist]
                # 2. Объединяем плоский список в ОДНУ БОЛЬШУЮ СТРОКУ с переносами строк
                full_data_string = "\n".join(flat_list)
                # 3. Читаем строку через StringIO в DataFrame
                df_hit = pd.read_csv(StringIO(full_data_string), sep='\t', low_memory=False)
                mask_header_row = (df_hit == df_hit.columns).all(axis=1)
                df_hit = df_hit[~mask_header_row]

            # Переименование столбцов
            df_hit.columns = df_hit.columns.str.replace(':', '_')
            df_hit.columns = df_hit.columns.str.lower()
            df_hit[['ym_pv_watchid','ym_pv_clientid','ym_pv_counteruseridhash']] = df_hit[['ym_pv_watchid','ym_pv_clientid','ym_pv_counteruseridhash']].astype(str)
            # Смена типа столбца
            df_hit['ym_pv_ipaddress'] = df_hit['ym_pv_ipaddress'].astype(str)
            df_hit.drop_duplicates(inplace=True)
            df_hit['ym_pv_datetime'] = pd.to_datetime(df_hit['ym_pv_datetime'], errors='coerce')
            df_hit['ym_pv_date'] = pd.to_datetime(df_hit['ym_pv_date'], errors='coerce').dt.date
        except Exception as err:
            logging.error(f"Oshibka chtenia dannih hits ili zapisi v dataframe - {err}", exc_info=True)



        # ======== Таблица ym_hits_obshee ========

        try:
            logging.info("Start zagruzka dannih - ym_hits_obshee.")
            col = df_hit.copy()[['ym_pv_datetime','ym_pv_watchid','ym_pv_clientid','ym_pv_counteruseridhash',
                                'ym_pv_pageviewid','ym_pv_counterid','ym_pv_date','ym_pv_title','ym_pv_pagecharset',
                                'ym_pv_url','ym_pv_regioncity','ym_pv_regioncountry','ym_pv_regioncityid',
                                'ym_pv_regioncountryid','ym_pv_ispageview']]
            # Пишем в PGSQL
            col.to_sql('ym_hits_obshee', engine, schema='ym', if_exists='append', index=False)
            logging.info("End zagruzka dannih - ym_hits_obshee.")
        except Exception as err:
                logging.error(f"Error: oshibka zagruzki v tablitsu - ym_hits_obshee - {err}", exc_info=True)



        # ======== Таблица ym_hits_ad ========

        try:
            logging.info("Start zagruzka dannih - ym_hits_ad.")
            col = df_hit.copy()[['ym_pv_datetime','ym_pv_watchid','ym_pv_clientid','ym_pv_counteruseridhash',
                                'ym_pv_referer', 'ym_pv_utmcampaign','ym_pv_utmcontent','ym_pv_utmmedium',
                                'ym_pv_utmsource','ym_pv_utmterm', 'ym_pv_openstatad','ym_pv_openstatcampaign',
                                'ym_pv_openstatservice','ym_pv_openstatsource', 'ym_pv_operatingsystem',
                                'ym_pv_from','ym_pv_hasgclid','ym_pv_gclid','ym_pv_lasttrafficsource',
                                'ym_pv_lastsearchengineroot','ym_pv_lastsearchengine','ym_pv_lastadvengine',
                                'ym_pv_lastsocialnetwork','ym_pv_lastsocialnetworkprofile',
                                'ym_pv_recommendationsystem','ym_pv_messenger']]
            # Пишем в PGSQL
            col.to_sql('ym_hits_ad', engine, schema='ym', if_exists='append', index=False)
            logging.info("End zagruzka dannih - ym_hits_ad.")
        except Exception as err:
                logging.error(f"Error: oshibka zagruzki v tablitsu - ym_hits_ad - {err}", exc_info=True)


        # ======== Таблица ym_hits_events ========

        try:
            logging.info("Start zagruzka dannih - ym_hits_events.")
            col = df_hit.copy()[['ym_pv_datetime','ym_pv_watchid','ym_pv_clientid','ym_pv_counteruseridhash',
                                'ym_pv_goalsid','ym_pv_iframe','ym_pv_link','ym_pv_download','ym_pv_notbounce',
                                'ym_pv_artificial','ym_pv_isturbopage','ym_pv_isturboapp','ym_pv_ecommerce',
                                'ym_pv_params','ym_pv_parsedparamskey2','ym_pv_parsedparamskey3',
                                'ym_pv_parsedparamskey4','ym_pv_parsedparamskey5','ym_pv_parsedparamskey6',
                                'ym_pv_parsedparamskey7','ym_pv_parsedparamskey8','ym_pv_parsedparamskey9',
                                'ym_pv_parsedparamskey10']]
            # Пишем в PGSQL
            col.to_sql('ym_hits_events', engine, schema='ym', if_exists='append', index=False)
            logging.info("End zagruzka dannih - ym_hits_events.")
        except Exception as err:
                logging.error(f"Error: oshibka zagruzki v tablitsu - ym_hits_events - {err}", exc_info=True)

        # ======== Таблица ym_hits_tech ========
                
        try:
            logging.info("Start zagruzka dannih - ym_hits_tehc.")
            col = df_hit.copy()[['ym_pv_datetime','ym_pv_watchid','ym_pv_clientid','ym_pv_counteruseridhash',
                                'ym_pv_browser','ym_pv_browsermajorversion','ym_pv_browsercountry',
                                'ym_pv_browserengine','ym_pv_browserengineversion1','ym_pv_browserengineversion2',
                                'ym_pv_browserengineversion3','ym_pv_browserengineversion4','ym_pv_browserlanguage',
                                'ym_pv_clienttimezone','ym_pv_cookieenabled','ym_pv_devicecategory',
                                'ym_pv_javascriptenabled','ym_pv_mobilephone','ym_pv_mobilephonemodel',
                                'ym_pv_operatingsystemroot','ym_pv_physicalscreenheight','ym_pv_physicalscreenwidth',
                                'ym_pv_screencolors','ym_pv_screenformat','ym_pv_screenheight',
                                'ym_pv_screenorientation','ym_pv_screenorientationname','ym_pv_screenwidth',
                                'ym_pv_windowclientheight','ym_pv_windowclientwidth','ym_pv_ipaddress',
                                'ym_pv_httperror','ym_pv_networktype','ym_pv_shareservice','ym_pv_shareurl',
                                'ym_pv_sharetitle']]
            # Пишем в PGSQL
            col.to_sql('ym_hits_tech', engine, schema='ym', if_exists='append', index=False)
            logging.info("End zagruzka dannih - ym_hits_tehc.")
        except Exception as err:
                logging.error(f"Error: oshibka zagruzki v tablitsu - ym_hits_tehc - {err}", exc_info=True)
            
    

    df = request_logs_ym(params_hits)
    logs_to_db(df, engine)


ym_hits_dag()


