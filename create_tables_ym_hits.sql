
-- ====== Таблица ym_hits_obshee ======

-- Создаем домен для UInt64 если его еще нет
-- По нормальному, создаются при создании базы
CREATE DOMAIN IF NOT EXISTS uint64 AS BIGINT CHECK (VALUE >= 0);

-- Создаем таблицу
CREATE TABLE ym.ym_hits_obshee (
    "ym_pv_datetime" TIMESTAMP NOT NULL, -- Временные метки (первым, т.к. часто используется в выборках)
    "ym_pv_watchid" NUMERIC NOT NULL, -- Основные идентификаторы (составной первичный ключ)
    "ym_pv_clientid" NUMERIC NOT NULL, -- Основные идентификаторы (составной первичный ключ)
    "ym_pv_counteruseridhash" NUMERIC NOT NULL, -- Основные идентификаторы (составной первичный ключ)
    "ym_pv_pageviewid" INTEGER,
    "ym_pv_counterid" INTEGER,
    "ym_pv_date" DATE NOT NULL,
    "ym_pv_title" TEXT,
    "ym_pv_pagecharset" VARCHAR(50),
    "ym_pv_url" TEXT,
    "ym_pv_regioncity" VARCHAR(100),
    "ym_pv_regioncountry" VARCHAR(2), -- ISO код страны (2 символа)
    "ym_pv_regioncityid" INTEGER,
    "ym_pv_regioncountryid" INTEGER,
    "ym_pv_ispageview" SMALLINT,
    PRIMARY KEY ("ym_pv_datetime", "ym_pv_watchid", "ym_pv_clientid", "ym_pv_counteruseridhash") -- Составной первичный ключ (в порядке важности для запросов)
);

-- Дополнительные индексы для ускорения запросов (помимо первичного ключа)
CREATE INDEX IF NOT EXISTS idx_ym_hits_obshee_date ON ym.ym_hits_obshee ("ym_pv_date");

-- Составные индексы для часто используемых комбинаций
CREATE INDEX IF NOT EXISTS idx_ym_hits_obshee_datetime ON ym.ym_hits_obshee ("ym_pv_datetime");
CREATE INDEX IF NOT EXISTS idx_ym_hits_obshee_watchid ON ym.ym_hits_obshee ("ym_pv_watchid");
CREATE INDEX IF NOT EXISTS idx_ym_hits_obshee_clientid ON ym.ym_hits_obshee ("ym_pv_clientid");
CREATE INDEX IF NOT EXISTS idx_ym_hits_obshee_counteruseridhash ON ym.ym_hits_obshee ("ym_pv_counteruseridhash");

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ym_obshee_join ON ym.ym_hits_obshee ('ym_pv_datetime', 'ym_pv_clientid', 'ym_pv_watchid', 'ym_pv_counteruseridhash');


-- ====== Таблица ym_hits_ad ======

-- Создаем таблицу
CREATE TABLE ym.ym_hits_ad (
    -- Временные метки (первым, т.к. часто используется в выборках)
    "ym_pv_datetime" TIMESTAMP NOT NULL,
    -- Основные идентификаторы (составной первичный ключ)
    "ym_pv_watchid" NUMERIC NOT NULL,
    "ym_pv_clientid" NUMERIC NOT NULL,
    "ym_pv_counteruseridhash" NUMERIC NOT NULL,
    "ym_pv_referer" varchar(500),
    "ym_pv_utmcampaign" varchar(500),
    "ym_pv_utmcontent" varchar(500),
    "ym_pv_utmmedium" varchar(500),
    "ym_pv_utmsource" varchar(500),
    "ym_pv_utmterm" varchar(500),
    "ym_pv_openstatad" varchar(500),
    "ym_pv_openstatcampaign" varchar(500),
    "ym_pv_openstatservice" varchar(500),
    "ym_pv_openstatsource" varchar(500),
    "ym_pv_operatingsystem" varchar(500),
    "ym_pv_from" varchar(500),
    "ym_pv_hasgclid" integer,
    "ym_pv_gclid" varchar(500),
    "ym_pv_lasttrafficsource" varchar(500),
    "ym_pv_lastsearchengineroot" varchar(500),
    "ym_pv_lastsearchengine" varchar(500),
    "ym_pv_lastadvengine" varchar(500),
    "ym_pv_lastsocialnetwork" varchar(500),
    "ym_pv_lastsocialnetworkprofile" varchar(500),
    "ym_pv_recommendationsystem" varchar(500),
    "ym_pv_messenger" varchar(500),
    -- Составной первичный ключ (в порядке важности для запросов)
    PRIMARY KEY ("ym_pv_datetime", "ym_pv_watchid", "ym_pv_clientid", "ym_pv_counteruseridhash")
);


-- Дополнительные индексы для ускорения запросов (помимо первичного ключа)
CREATE INDEX IF NOT EXISTS idx_ym_hits_ad_hasGCLID ON ym.ym_hits_ad ("ym_pv_hasgclid") WHERE ym_pv_utmcampaign IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ym_hits_ad_UTMSource ON ym.ym_hits_ad ("ym_pv_utmsource") WHERE ym_pv_utmcampaign IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ym_hits_ad_utmcampaign ON ym.ym_hits_ad ("ym_pv_utmcampaign") WHERE ym_pv_utmcampaign IS NOT NULL;

-- Составные индексы для часто используемых комбинаций
CREATE INDEX IF NOT EXISTS idx_ym_hits_ad_datetime ON ym.ym_hits_ad ("ym_pv_datetime");
CREATE INDEX IF NOT EXISTS idx_ym_hits_ad_watchid ON ym.ym_hits_ad ("ym_pv_watchid");
CREATE INDEX IF NOT EXISTS idx_ym_hits_ad_clientid ON ym.ym_hits_ad ("ym_pv_clientid");
CREATE INDEX IF NOT EXISTS idx_ym_hits_ad_counteruseridhash ON ym.ym_hits_ad ("ym_pv_counteruseridhash");

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ym_ad_join ON ym.ym_hits_ad ('ym_pv_datetime', 'ym_pv_clientid,' 'ym_pv_watchid', 'ym_pv_counteruseridhash');



-- ====== ym_hits_events ======

-- Создаем таблицу
CREATE TABLE ym.ym_hits_events (
    -- Временные метки (первым, т.к. часто используется в выборках)
    "ym_pv_datetime" TIMESTAMP NOT NULL,
    -- Основные идентификаторы (составной первичный ключ)
    "ym_pv_watchid" NUMERIC NOT NULL,
    "ym_pv_clientid" NUMERIC NOT NULL,
    "ym_pv_counteruseridhash" NUMERIC NOT NULL,
    "ym_pv_goalsid" integer ARRAY,
    "ym_pv_iframe" integer,
    "ym_pv_link" integer,
    "ym_pv_download" integer,
    "ym_pv_notbounce" integer,
    "ym_pv_artificial" integer,
    "ym_pv_isturbopage" integer,
    "ym_pv_isturboapp" integer,
    "ym_pv_ecommerce" varchar(500),
    "ym_pv_params" varchar(1000),
    "ym_pv_parsedparamskey1" TEXT[],
    "ym_pv_parsedparamskey2" TEXT[],
    "ym_pv_parsedparamskey3" TEXT[],
    "ym_pv_parsedparamskey4" TEXT[],
    "ym_pv_parsedparamskey5" TEXT[],
    "ym_pv_parsedparamskey6" TEXT[],
    "ym_pv_parsedparamskey7" TEXT[],
    "ym_pv_parsedparamskey8" TEXT[],
    "ym_pv_parsedparamskey9" TEXT[],
    "ym_pv_parsedparamskey10" TEXT[],
    -- Составной первичный ключ (в порядке важности для запросов)
    PRIMARY KEY ("ym_pv_datetime", "ym_pv_watchid", "ym_pv_clientid", "ym_pv_counteruseridhash")
);


-- Составные индексы для часто используемых комбинаций
CREATE INDEX IF NOT EXISTS idx_ym_hits_events_datetime ON ym.ym_hits_events ("ym_pv_datetime");
CREATE INDEX IF NOT EXISTS idx_ym_hits_events_watchid ON ym.ym_hits_events ("ym_pv_watchid");
CREATE INDEX IF NOT EXISTS idx_ym_hits_events_clientid ON ym.ym_hits_events ("ym_pv_clientid");
CREATE INDEX IF NOT EXISTS idx_ym_hits_events_counteruseridhash ON ym.ym_hits_events ("ym_pv_counteruseridhash");

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ym_events_join ON ym.ym_hits_events ('ym_pv_datetime', 'ym_pv_clientid', 'ym_pv_watchid', 'ym_pv_counteruseridhash');



-- ====== ym_hits_tech ======

-- Создаем таблицу
CREATE TABLE ym.ym_hits_tech (
    -- Временные метки (первым, т.к. часто используется в выборках)
    "ym_pv_datetime" TIMESTAMP NOT NULL,
    -- Основные идентификаторы (составной первичный ключ)
    "ym_pv_watchid" NUMERIC NOT NULL,
    "ym_pv_clientid" NUMERIC NOT NULL,
    "ym_pv_counteruseridhash" NUMERIC NOT NULL,
    "ym_pv_browser" varchar(500),
    "ym_pv_browsermajorversion" int,
    "ym_pv_browsercountry" varchar(500),
    "ym_pv_browserenginee" varchar(500),
    "ym_pv_browserengineversion1" int,
    "ym_pv_browserengineversion2" int,
    "ym_pv_browserengineversion3" int,
    "ym_pv_browserengineversion4" int,
    "ym_pv_browserlanguage" varchar(500),
    "ym_pv_clienttimezone" int,
    "ym_pv_cookieenabled" int,
    "ym_pv_devicecategory" varchar(50),
    "ym_pv_javascriptenabled" int,
    "ym_pv_mobilephone" varchar(500),
    "ym_pv_mobilephonemodel" varchar(500),
    "ym_pv_operatingsystemroot" varchar(500),
    "ym_pv_physicalscreenheight" int,
    "ym_pv_physicalscreenwidth" int,
    "ym_pv_screencolors" int,
    "ym_pv_screenformat" varchar(50),
    "ym_pv_screenheight" int,
    "ym_pv_screenorientation" int,
    "ym_pv_screenorientationname" varchar(500),
    "ym_pv_screenwidth" int,
    "ym_pv_windowclientheight" int,
    "ym_pv_windowclientwidth" int,
    "ym_pv_ipaddress" varchar(500),
    "ym_pv_httperror" varchar(500),
    "ym_pv_networktype" varchar(500),
    "ym_pv_shareservice" varchar(500),
    "ym_pv_shareurl" varchar(500),
    "ym_pv_sharetitle" varchar(500),
    -- Составной первичный ключ (в порядке важности для запросов)
    PRIMARY KEY ("ym_pv_datetime", "ym_pv_watchid", "ym_pv_clientid", "ym_pv_counteruseridhash")
);
 

-- Дополнительные индексы для ускорения запросов (помимо первичного ключа)
CREATE INDEX IF NOT EXISTS idx_ym_hits_deviceCategory ON ym.ym_hits_tech ("ym_pv_browsercountry");
CREATE INDEX IF NOT EXISTS idx_ym_hits_browserLanguage ON ym.ym_hits_tech ("ym_pv_browserlanguage");
CREATE INDEX IF NOT EXISTS idx_ym_hits_clientTimeZone ON ym.ym_hits_tech ("ym_pv_clienttimezone");

-- Составные индексы для часто используемых комбинаций
CREATE INDEX IF NOT EXISTS idx_ym_hits_tech_datetime ON ym.ym_hits_tech ("ym_pv_datetime");
CREATE INDEX IF NOT EXISTS idx_ym_hits_tech_watchid ON ym.ym_hits_tech ("ym_pv_watchid");
CREATE INDEX IF NOT EXISTS idx_ym_hits_tech_clientid ON ym.ym_hits_tech ("ym_pv_clientid");
CREATE INDEX IF NOT EXISTS idx_ym_hits_tech_counteruseridhash ON ym.ym_hits_tech ("ym_pv_counteruseridhash");

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ym_tech_join ON ym.ym_hits_tech ('ym_pv_datetime', 'ym_pv_clientid', 'ym_pv_watchid', 'ym_pv_counteruseridhash');














