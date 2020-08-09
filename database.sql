
CREATE TABLE targets (
  target_id         SERIAL NOT NULL PRIMARY KEY,
  time_added        TEXT NOT NULL,
  target_type       TEXT NOT NULL,
  day_added         TEXT NOT NULL,
  experiment_name   TEXT NOT NULL,
  url               TEXT NOT NULL,
  day_repeated      TEXT NOT NULL,            
  UNIQUE (day_added, url, target_type, experiment_name)
);
CREATE INDEX day_added ON targets("day_added");

CREATE TABLE pharma_targets (
  pharma_id         SERIAL NOT NULL PRIMARY KEY,
  target_id         INTEGER NOT NULL REFERENCES targets(target_id),
  keywords          TEXT NOT NULL,
  raw_data          BYTEA NOT NULL,
  total_results     BIGINT NOT NULL,
  title             TEXT NOT NULL,
  link              TEXT NOT NULL,
  display_link      TEXT NOT NULL,
  snippet           TEXT NOT NULL,
  rank              INTEGER NOT NULL,
  meta_tag          TEXT NOT NULL
);

CREATE TABLE typo_targets (
  typo_id           SERIAL NOT NULL PRIMARY KEY,
  target_id         INTEGER NOT NULL REFERENCES targets(target_id),
  typo_domain       TEXT NOT NULL,
  original_domain   TEXT NOT NULL,
  alexa_rank        INTEGER NOT NULL,
  mistake_type      TEXT NOT NULL,
  name_server       TEXT NOT NULL,
  weight            INTEGER NOT NULL
);

CREATE TABLE list_targets (
  list_id         SERIAL NOT NULL PRIMARY KEY,
  target_id       INTEGER NOT NULL REFERENCES targets(target_id),
  meta_data        BYTEA NOT NULL
);


CREATE TABLE run_config (
  config_id     SERIAL NOT NULL PRIMARY KEY,
  run_config    BYTEA
);

CREATE TABLE scrapes (
  scrape_id                 SERIAL NOT NULL PRIMARY KEY,
  target_id                 INTEGER NOT NULL REFERENCES targets(target_id),
  config_id                 INTEGER REFERENCES run_config(config_id),
  name                      TEXT NOT NULL,
  scrape_location           TEXT NOT NULL,
  useragent                 TEXT NOT NULL,
  referrer                  TEXT NOT NULL,
  browser_type              TEXT NOT NULL,
  mobile_emulation          BOOLEAN NOT NULL,     
  scrape_time               TEXT NOT NULL,
  har                       BYTEA,  
  performance_log           BYTEA,
  html                      BYTEA,
  screenshot                BYTEA,
  after_click_urls          BYTEA,
  after_click_har           BYTEA,
  after_click_htmls         BYTEA,
  after_click_screenshots   BYTEA,
  after_click_landing_urls  BYTEA,
  after_click_perflogs      BYTEA,
  landing_url               TEXT,
  http_proxy                TEXT
);  

CREATE TABLE perceptual_hashes (
  hash_id       SERIAL NOT NULL PRIMARY KEY,
  target_id     INTEGER NOT NULL REFERENCES targets(target_id),
  scrape_id     INTEGER NOT NULL REFERENCES scrapes(scrape_id),
  hash          TEXT NOT NULL,
  maliciousness TEXT NOT NULL,
  window_id     INTEGER  
);

CREATE TABLE redirect_stats (
  redirect_stats_id             SERIAL NOT NULL PRIMARY KEY,
  target_id                     INTEGER NOT NULL,
  scrape_id                     INTEGER NOT NULL,
  crawl_type                    TEXT NOT NULL,
  redirect_chain                BYTEA NOT NULL,
  dom_redirect_chain            BYTEA NOT NULL,
  redirect_chain_len            INTEGER NOT NULL,
  dom_redirect_chain_len        INTEGER NOT NULL,
  content_distribution          BYTEA NOT NULL,
  dom_content_distribution      BYTEA NOT NULL,
  guessed_last_url              TEXT,
  landing_domain                TEXT NOT NULL,
  landing_dom_content_size      INTEGER NOT NULL, 
  max_content_domain            TEXT NOT NULL,
  max_content_dom_content_size  INTEGER NOT NULL,
  total_content_size            INTEGER NOT NULL,
  error_codes                   BYTEA NOT NULL,
  window_id                     INTEGER,
  is_main_frame                 TEXT  
);

CREATE TABLE tagging_final (
  tag_id        SERIAL NOT NULL PRIMARY KEY,
  target_id     INTEGER NOT NULL,
  scrape_id     INTEGER NOT NULL,
  window_id     INTEGER,
  tag           TEXT NOT NULL,
  tag_source    TEXT NOT NULL,
  secondary_tag TEXT,
  comment       TEXT
);

CREATE TABLE tagging_extrapolated_final (
  tag_id        SERIAL NOT NULL PRIMARY KEY,
  target_id     INTEGER NOT NULL,
  scrape_id     INTEGER NOT NULL,
  window_id     INTEGER,
  tag           TEXT NOT NULL,
  tag_source    TEXT NOT NULL,
  secondary_tag TEXT,
  comment       TEXT
);

CREATE TABLE redirection_features (
  redirection_feature_id    SERIAL NOT NULL PRIMARY KEY,
  target_id                 INTEGER NOT NULL,
  scrape_id                 INTEGER NOT NULL,
  window_id                 INTEGER,
  --main
  n_redir_hops              TEXT,
  n_dom_redir_hops          TEXT,
  is_ips                    TEXT,
  tlds                      TEXT,
  redir_types               TEXT,
  n_ips                     TEXT,
  --additional doamin features
  cur_dom_lens              TEXT, 
  cur_n_hyphens             TEXT,
  cur_n_domain_dots         TEXT,
  dom_lens                  TEXT, 
  n_hyphens                 TEXT,
  n_domain_dots             TEXT,
  -- additional url features
  cur_url_lens              TEXT,
  cur_n_params              TEXT,
  cur_param_lens            TEXT,
  cur_dir_lens              TEXT,
  cur_n_subdirs             TEXT,
  cur_len_filenames         TEXT,  
  cur_content_sizes         TEXT,
  url_lens                  TEXT,
  n_params                  TEXT,
  param_lens                TEXT,
  dir_lens                  TEXT,
  n_subdirs                 TEXT,
  len_filenames             TEXT,  
  content_sizes             TEXT
);
  