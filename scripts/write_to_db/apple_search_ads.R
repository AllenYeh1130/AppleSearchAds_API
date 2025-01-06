# ===
# 從Apple Search Ads 獲得 campaign、keyword 資訊，並匯入資料庫
project_name <- 'apple_search_ads'
# ===

# Loading Package
library(magrittr)        # pipeline
library(lubridate)       # date manipulation
library(dplyr)           # data manipulation
library(yaml)            # reading config.yml
library(RMySQL)          # mySQL
library(futile.logger)   # log file
library(futile.options)  # log file
library(stringr)         # split text
library(httr)            # get api data
library(jsonlite)        # transform json data
library(mailR)           # send error mail 
options(scipen = 20)     
options(warn = FALSE)
options(encoding = "UTF-8")

# 匯入DB限制、翻頁限制
partition_n = 10000
pagination_limit = 1000

# 時間參數
args <- commandArgs(trailingOnly = TRUE)
exe_datetime <- as.character(Sys.time() %>% format(.,  "%Y%m%d%H%M%S"))
start_date <- args[1] %>% as.Date
end_date <- args[2] %>% as.Date  
start_date <- dplyr::if_else(is.na(start_date), Sys.Date() - 1, start_date)
end_date <- dplyr::if_else(is.na(end_date), Sys.Date(), end_date)
# start_date <- Sys.Date() - 1
# end_date <- Sys.Date()

# 執行路徑
workpath <- "C:/git/Apple_search_ads_API/scripts/write_to_db"
setwd(workpath)

# log檔路徑
logpath <- paste0(sprintf("C:/git/Apple_search_ads_API/logs/%s.log",project_name))
if (file.exists(logpath) == FALSE){
  file.create(logpath)
}
# 啟動log
flog.logger(appender = appender.file(logpath), name = project_name)

# 設定檔載入
all_config <- yaml.load_file('C:/git/Apple_search_ads_API/configs/all_config.yml')
asa_config <- yaml.load_file('C:/git/Apple_search_ads_API/configs/ASA_config.yml')

# 連資料庫
connect_DB <- dbConnect(RMySQL::MySQL(),
                        host     = all_config$db$host,
                        dbname   = all_config$db$name,
                        dbport   = all_config$db$port,
                        username = all_config$db$user,
                        password = all_config$db$pass)

# 設定MySQL連線編碼
dbSendQuery(connect_DB,'SET NAMES utf8')


tryCatch({
  # Campaign Report
  flog.info(paste0(exe_datetime, " Work start apple_search_ads_campaign"), name = project_name)
  
  # ===== 1. ASA認證 =====
  # API: Access token 獲取
  client_id <- asa_config$apple_search_ads$client_id
  client_secret <- asa_config$apple_search_ads$client_secret
  access_url <- sprintf("https://appleid.apple.com/auth/oauth2/token?grant_type=client_credentials&client_id=%s&client_secret=%s&scope=searchadsorg",client_id ,client_secret)
  asa_access_token_request <- httr::POST(access_url,
                                         add_headers(
                                           Host = "appleid.apple.com",
                                           `Content-Type` = "application/x-www-form-urlencoded"
                                           )
                                         ,timeout(180))
  
  # Access token JSON轉換
  asa_access_token  <- asa_access_token_request$content %>% rawToChar %>% jsonlite::fromJSON(flatten = TRUE) %>% .$access_token
  
  # Bearer token 建立
  api_Authorization <- sprintf('Bearer %s', asa_access_token)
  
  # 帳號資訊
  acl_url <- sprintf("https://api.searchads.apple.com/api/%s/acls", asa_config$apple_search_ads$api_version)
  orgId_request <- httr::GET(acl_url, add_headers(Authorization = api_Authorization),timeout(180))
  
  # orgId JSON轉換
  orgId_list <- fromJSON(orgId_request$content %>% rawToChar) %>% .$data %>% .$orgId
  
  # 判斷是否有帳號
  if (length(orgId_list) > 0) {
    
    # ===== 2. 查詢各廣告帳號 =====
    # orgId <- '123456'
    for (orgId in orgId_list) {
      flog.info(paste0(exe_datetime, sprintf(' Apple search ads orgId: %s update start !!', orgId)), name = project_name)
      print(sprintf(' Apple search ads orgId: %s update start !!', orgId))

      # ===== 3. 確認campaign數據 ===== 
      api_url <- sprintf("https://api.searchads.apple.com/api/%s/reports/campaigns", asa_config$apple_search_ads$api_version)
      granularity <- 'DAILY'
      groupBy <- '["countryOrRegion"]'
      selector_orderBy <- '[{"field":"impressions","sortOrder":"DESCENDING"}]'
      # 之後可能會有分頁問題
      selector_pagination <- sprintf('{"offset":0,"limit": 1000}')
      report_body <- sprintf('{"startTime": "%s", "endTime": "%s",
                             "granularity": "%s", "groupBy": %s,
                             "selector": {"orderBy": %s, "pagination": %s}}',
                             start_date, end_date, granularity, groupBy,
                             selector_orderBy, selector_pagination)
      
      # POST
      campaign_request <- httr::POST(api_url, 
                                     body = report_body,
                                     add_headers(
                                       Authorization = api_Authorization,
                                       `X-AP-Context` = sprintf("orgId=%s", orgId),
                                       `Content-Type` = "application/json"),
                                     timeout(180), 
                                     verbose())
      Sys.sleep(5)
      
      # 判斷伺服器回應
      if (campaign_request$status_code == 200){
        # 轉換JSON
        campaign_json_data  <- campaign_request$content %>% rawToChar %>% jsonlite::fromJSON(flatten = TRUE) %>%
          .$data %>% .$reportingDataResponse %>% .$row
        
        # 檢查是否有資料，取得時間內有資料的campaign_id
        if (is.data.frame(campaign_json_data)){
          # 檢查資料欄位
          names(campaign_json_data) %<>% str_replace(., "metadata.", "")
          campaign_data <- campaign_json_data %>% apply(1, function(x){
            campaign_data <- x$granularity %>% as.data.frame() %>% select(date, impressions, taps, installs, newDownloads, redownloads, localSpend.amount)
            campaign_data$app_id <- x$app.adamId %>% paste0("id", .)
            campaign_data$campaign_id <- x$campaignId
            campaign_data$campaign_name <- x$campaignName
            campaign_data$adChannelType <- x$adChannelType
            campaign_data$country_code <- ifelse(is.na(x$countryOrRegion[[1]]), "unknown", x$countryOrRegion[[1]])
            return(campaign_data)
          }) %>% do.call(plyr::rbind.fill, .) %>% dplyr::mutate(account_id = orgId)
          
          # campiang基本資訊
          campaign <- campaign_data %>% select(account_id, campaign_id, campaign_name, adChannelType) %>% unique()

          # ===== 4. 查詢各廣告帳號的各campaign詳細資訊 ===== 
          # campaign_id <- '123456789'
          for (campaign_id in unique(campaign$campaign_id)) {
            flog.info(paste0(exe_datetime, sprintf(' Apple search ads orgId: %s, campaign_id: %s, update start !!', orgId, campaign_id)), name = project_name)
            print(sprintf(' Apple search ads orgId: %s, campaign_id: %s, update start !!', orgId, campaign_id))
            
            # ===== 5. 區分是搜尋或是瀏覽campaign =====
            if (campaign[campaign$campaign_id == campaign_id,]$adChannelType == 'SEARCH') {
              # while停止設定
              stop_while <- 0
              # 執行分頁次數
              paging_cnt <- 0
              # 目前完成資料數
              itemsPerPage <- 0
              
              while (stop_while == 0) {
                # 起始資料數
                offset <- 0+(pagination_limit*paging_cnt)
                
                # ===== 5-1-1. 搜尋campaign詳細數據 (keyword_id) =====
                api_url <- sprintf("https://api.searchads.apple.com/api/%s/reports/campaigns/%s/keywords", asa_config$apple_search_ads$api_version, campaign_id)
                granularity <- 'DAILY'
                groupBy <- '["countryOrRegion"]'
                selector_orderBy <- '[{"field":"impressions","sortOrder":"DESCENDING"}]'
                selector_pagination <- sprintf('{"offset":%s,"limit": %s}', offset, pagination_limit)
                report_body <- sprintf('{"startTime": "%s", "endTime": "%s",
                                       "granularity": "%s", "groupBy": %s,
                                       "selector": {"orderBy": %s, "pagination": %s}}',
                                       start_date, end_date, granularity, groupBy,
                                       selector_orderBy, selector_pagination)
                
                # POST
                apple_search_ads_request <- httr::POST(api_url, 
                                                       body = report_body,
                                                       add_headers(
                                                         Authorization = api_Authorization,
                                                         `X-AP-Context` = sprintf("orgId=%s", orgId),
                                                         `Content-Type` = "application/json"),
                                                       timeout(180), 
                                                       verbose())
                Sys.sleep(1)
                
                if (apple_search_ads_request$status_code == 200){
                  # 轉換JSON
                  keyword_json_data  <- apple_search_ads_request$content %>% rawToChar %>% jsonlite::fromJSON(flatten = TRUE) %>%
                    .$data %>% .$reportingDataResponse %>% .$row
                  
                  # ===== 5-1-2. 檢查是否有資料，取得時間內各campaign的keyword資訊，無keyword資料則是搜尋配對，在adgroup層再處理 =====
                  if (is.data.frame(keyword_json_data)){
                    # 檢查資料欄位
                    names(keyword_json_data) %<>% str_replace(., "metadata.", "")
                    keyword_data <- keyword_json_data %>% apply(1, function(x){
                      keyword_data <- x$granularity %>% as.data.frame() %>% select(date, impressions, taps, installs, newDownloads, redownloads, localSpend.amount)
                      keyword_data$adgroup_id <- x$adGroupId
                      keyword_data$adgroup_name <- x$adGroupName
                      keyword_data$keyword_id <- x$keywordId
                      keyword_data$keyword_name <- x$keyword
                      keyword_data$match_type <- x$matchType
                      keyword_data$country_code <- ifelse(is.na(x$countryOrRegion[[1]]), "unknown", x$countryOrRegion[[1]])
                      return(keyword_data)
                    }) %>% do.call(plyr::rbind.fill, .) %>% dplyr::mutate(campaign_id = as.integer(campaign_id))
                    
                    # 排除單引號的字元，避免組SQL出錯
                    keyword_data$keyword_name <- gsub("'", "", keyword_data$keyword_name)
                    
                    # 排除沒資料的數據
                    keyword_data %<>% filter(impressions != 0 | taps != 0 | installs != 0 | localSpend.amount != 0)
    
                    # ===== 5-1-3. campaign資料匯入DB =====
                    # 根據campaign資訊調整名稱跟欄位
                    campaign_insight <- keyword_data %>% inner_join(campaign, by = 'campaign_id') %>%
                      select(account_id, campaign_id, campaign_name, adgroup_id, adgroup_name,
                             keyword_id, keyword_name, match_type) %>% unique()
                    
                    # 匯入資料庫
                    if (nrow(campaign_insight) > 0){
                      flog.info(paste0(exe_datetime, sprintf('匯入ASA campaign資料，共%s筆。', nrow(campaign_insight))), name = project_name)
                      print(sprintf('匯入ASA campaign資料，共%s筆。', nrow(campaign_insight)))
                      
                      # 將資料寫入(SQL版本)
                      campaign_insight_values <- campaign_insight %>%
                        apply(., 1, function(x){
                          output <- x %>% 
                            paste0(., collapse = "', '") %>%
                            paste0("('", ., "')")
                          
                          return(output) }) %>%
                        paste0(., collapse = ",")
                      
                      # 將空格都排除，keyword_id等id是字串，數字進位後其它未進位的值會帶空白，是apply的原因
                      campaign_insight_values <- gsub("' ", "'", campaign_insight_values, fixed = TRUE)
                      campaign_insight_SQL <- sprintf("INSERT mobile_game_asa_campaign (%s) VALUES ", 
                                                      paste0(names(campaign_insight), collapse = ", "))
                      
                      # ON DUPLICATE KEY UPDATE 組字串
                      DUPLICATE_KEY_UPDATE_SQL <- names(campaign_insight) %>% paste0(" = VALUES(",.,")") %>% 
                        paste0(names(campaign_insight),.) %>%
                        paste0(collapse = " , ") %>% 
                        paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
                      insert_campaign_insight_SQL <- paste0(campaign_insight_SQL, campaign_insight_values, DUPLICATE_KEY_UPDATE_SQL)
                      dbSendQuery(connect_DB, insert_campaign_insight_SQL)
                    }
                    
                    # ===== 5-1-4. 詳細keyword資料匯入DB =====
                    # 根據keyword詳細資訊調整名稱跟欄位
                    keyword_insight <- keyword_data %>% inner_join(campaign %>% select(campaign_id), by = 'campaign_id') %>%
                      rename(clicks = taps, new_downloads = newDownloads, cost = localSpend.amount) %>%
                      select(-c(adgroup_name,keyword_name, match_type))
                    
                    # 匯入資料庫
                    if (nrow(keyword_insight) > 0){
                      flog.info(paste0(exe_datetime, sprintf('匯入ASA keyword詳細資料，共%s筆。', nrow(keyword_insight))), name = project_name)
                      print(sprintf('匯入ASA keyword詳細資料，共%s筆。', nrow(keyword_insight)))
                      
                      # 資料分批寫入(SQL版本)
                      insert_values <- NULL
                      partition_seq <- seq(1, nrow(keyword_insight), by = partition_n)
                      
                      for (i in 1:length(partition_seq)) {
                        start_ind = partition_seq[i]
                        end_ind = partition_seq[i] - 1 + partition_n
                        
                        if (end_ind > nrow(keyword_insight)) {
                          end_ind = nrow(keyword_insight)
                        }
                        
                        insert_values[i] <- keyword_insight[start_ind:end_ind,] %>%
                          apply(., 1, function(x){
                            output <- x %>%
                              paste0(., collapse = "', '") %>%
                              paste0("('", ., "')")
                            
                            return(output) }) %>%
                          paste0(., collapse = ",")
                        
                        # 將空格都排除，keyword_id等id是字串，數字進位後其它未進位的值會帶空白，是apply的原因
                        insert_values[i] <- gsub("' ", "'", insert_values[i], fixed = TRUE)
                      }
                      keyword_insight_SQL <- sprintf("INSERT mobile_game_asa_insight (%s) VALUES ", paste0(names(keyword_insight), collapse = ", "))
                      
                      # ON DUPLICATE KEY UPDATE 組字串
                      DUPLICATE_KEY_UPDATE_SQL <- names(keyword_insight) %>% paste0(" = VALUES(",.,")") %>% 
                        paste0(names(keyword_insight),.) %>%
                        paste0(collapse = " , ") %>% 
                        paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
                      
                      for (i in 1:length(insert_values)) {
                        insert_keyword_insight_SQL <- paste0(keyword_insight_SQL, insert_values[i], DUPLICATE_KEY_UPDATE_SQL)
                        dbSendQuery(connect_DB, insert_keyword_insight_SQL)
                      }
                    }
                  }
                  
                  # ===== 5-1-5. 確認是否有分頁 =====
                  pagination_data  <- apple_search_ads_request$content %>% rawToChar %>% jsonlite::fromJSON(flatten = TRUE) %>% .$pagination
                  # 目前已完成資料數目
                  itemsPerPage <-  pagination_data$itemsPerPage + (pagination_limit*paging_cnt)
                  # 判斷while迴圈是否繼續POST
                  if (pagination_data$totalResults > itemsPerPage) {
                    stop_while <- 0
                    paging_cnt <- paging_cnt+1
                    flog.info(paste0(exe_datetime, sprintf('有多於1000筆資料，執行第%s次while迴圈拉後續資料', paging_cnt)), name = project_name)
                    print(sprintf('有多於1000筆資料，執行第%s次while迴圈拉後續資料', paging_cnt))
                  } else {
                    stop_while <- 1
                  }
                } else {
                  # 伺服器端有問題
                  error_message <- apple_search_ads_request %>% content %>% .$error %>% .$errors %>% `[[`(1) %>% 
                  { paste(.$messageCode, " : ", .$message) }
                  
                  stop(paste(exe_datetime, apple_search_ads_request$status_code, error_message))
                }
              }
              
              # ===== 5-2-1. 搜尋配對詳細數據 (adgroup_id) =====
              api_url <- sprintf("https://api.searchads.apple.com/api/%s/reports/campaigns/%s/adgroups", asa_config$apple_search_ads$api_version, campaign_id)
              granularity <- 'DAILY'
              groupBy <- '["countryOrRegion"]'
              selector_orderBy <- '[{"field":"impressions","sortOrder":"DESCENDING"}]'
              selector_pagination <- sprintf('{"offset":0,"limit": 1000}')
              report_body <- sprintf('{"startTime": "%s", "endTime": "%s",
                                     "granularity": "%s", "groupBy": %s,
                                     "selector": {"orderBy": %s, "pagination": %s}}',
                                     start_date, end_date, granularity, groupBy,
                                     selector_orderBy, selector_pagination)
              
              # POST  
              apple_search_ads_request <- httr::POST(api_url, 
                                                     body = report_body,
                                                     add_headers(
                                                       Authorization = api_Authorization,
                                                       `X-AP-Context` = sprintf("orgId=%s", orgId),
                                                       `Content-Type` = "application/json"),
                                                     timeout(180), 
                                                     verbose())
                
              if (apple_search_ads_request$status_code == 200){
                # 轉換JSON
                adgroup_json_data  <- apple_search_ads_request$content %>% rawToChar %>% jsonlite::fromJSON(flatten = TRUE) %>%
                  .$data %>% .$reportingDataResponse %>% .$row
                  
                if (is.data.frame(adgroup_json_data)){
                  names(adgroup_json_data) %<>% str_replace(., "metadata.", "")
                  
                  # 檢查資料欄位
                  for (i in 1:nrow(adgroup_json_data)) {
                    # 對每筆adgroup進行檢查，符合條件則為搜尋配對，進行資料處理
                    if (adgroup_json_data[i,]$automatedKeywordsOptIn==TRUE) {
                      adgroup_data <- adgroup_json_data[i,] %>% apply(1, function(x){
                        adgroup_data <- x$granularity %>% as.data.frame() %>% select(date, impressions, taps, installs, newDownloads, redownloads, localSpend.amount)
                        adgroup_data$adgroup_id <- x$adGroupId
                        adgroup_data$adgroup_name <- x$adGroupName
                        adgroup_data$match_type <- x$matchType
                        adgroup_data$country_code <- ifelse(is.na(x$countryOrRegion[[1]]), "unknown", x$countryOrRegion[[1]])
                        return(adgroup_data)
                      }) %>% do.call(plyr::rbind.fill, .) %>% dplyr::mutate(campaign_id = as.integer(campaign_id))
                      
                      # 排除沒資料的數據
                      adgroup_data %<>% filter(impressions != 0 | taps != 0 | installs != 0 | localSpend.amount != 0)
                      
                      # ===== 5-2-2. 搜尋配對campaign資料匯入DB =====
                      # 根據campaign資訊調整名稱跟欄位
                      search_campaign <- adgroup_data %>% inner_join(campaign, by = 'campaign_id') %>%
                        select(account_id, campaign_id, campaign_name, adgroup_id, adgroup_name) %>% 
                        dplyr::mutate(keyword_id = '', keyword_name = '', match_type = 'SEARCH') %>% unique()
                      
                      # 匯入資料庫
                      if (nrow(search_campaign) > 0){
                        flog.info(paste0(exe_datetime, sprintf('匯入ASA 搜尋配對campaign資料，共%s筆。', nrow(search_campaign))), name = project_name)
                        print(sprintf('匯入ASA 搜尋配對campaign資料，共%s筆。', nrow(search_campaign)))
                        
                        # 將資料寫入(SQL版本)
                        search_campaign_values <- search_campaign %>%
                          apply(., 1, function(x){
                            output <- x %>% 
                              paste0(., collapse = "', '") %>%
                              paste0("('", ., "')")
                            
                            return(output) }) %>%
                          paste0(., collapse = ",")
                        # 將空格都排除，keyword_id等id是字串，數字進位後其它未進位的值會帶空白，是apply的原因
                        search_campaign_values <- gsub("' ", "'", search_campaign_values, fixed = TRUE)
                        
                        search_campaign_SQL <- sprintf("INSERT mobile_game_asa_campaign (%s) VALUES ", 
                                                       paste0(names(search_campaign), collapse = ", "))
                        
                        # ON DUPLICATE KEY UPDATE 組字串
                        DUPLICATE_KEY_UPDATE_SQL <- names(search_campaign) %>% paste0(" = VALUES(",.,")") %>% 
                          paste0(names(search_campaign),.) %>%
                          paste0(collapse = " , ") %>% 
                          paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
                        insert_search_campaign_SQL <- paste0(search_campaign_SQL, search_campaign_values, DUPLICATE_KEY_UPDATE_SQL)
                        dbSendQuery(connect_DB, insert_search_campaign_SQL)
                      }
                      
                      # ===== 5-2-3. 搜尋配對詳細資料匯入DB =====
                      # 根據adgroup詳細資訊調整名稱跟欄位
                      search_insight <- adgroup_data %>% inner_join(campaign %>% select(campaign_id), by = 'campaign_id') %>%
                        rename(clicks = taps, new_downloads = newDownloads, cost = localSpend.amount) %>% dplyr::mutate(keyword_id = '') %>%
                        select(-c(adgroup_name))
                      
                      # 排除沒資料的數據
                      search_insight %<>% filter(impressions != 0 | clicks != 0 | installs != 0 | cost != 0)
                      
                      # 匯入資料庫
                      if (nrow(search_insight) > 0){
                        flog.info(paste0(exe_datetime, sprintf('匯入ASA 搜尋配對詳細資料，共%s筆。', nrow(search_insight))), name = project_name)
                        print(sprintf('匯入ASA 搜尋配對詳細資料，共%s筆。', nrow(search_insight)))
                        
                        # 資料分批寫入(SQL版本)
                        insert_values <- NULL
                        partition_seq <- seq(1, nrow(search_insight), by = partition_n)
                        
                        for (i in 1:length(partition_seq)) {
                          start_ind = partition_seq[i]
                          end_ind = partition_seq[i] - 1 + partition_n
                          
                          if (end_ind > nrow(search_insight)) {
                            end_ind = nrow(search_insight)
                          }
                          
                          insert_values[i] <- search_insight[start_ind:end_ind,] %>%
                            apply(., 1, function(x){
                              output <- x %>%
                                paste0(., collapse = "', '") %>%
                                paste0("('", ., "')")
                              
                              return(output) }) %>%
                            paste0(., collapse = ",")
                          
                          # 將空格都排除，keyword_id等id是字串，數字進位後其它未進位的值會帶空白，是apply的原因
                          insert_values[i] <- gsub("' ", "'", insert_values[i], fixed = TRUE)
                        }
                        search_insight_SQL <- sprintf("INSERT mobile_game_asa_insight (%s) VALUES ", paste0(names(search_insight), collapse = ", "))
                        
                        # ON DUPLICATE KEY UPDATE 組字串
                        DUPLICATE_KEY_UPDATE_SQL <- names(search_insight) %>% paste0(" = VALUES(",.,")") %>% 
                          paste0(names(search_insight),.) %>%
                          paste0(collapse = " , ") %>% 
                          paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
                        
                        for (i in 1:length(insert_values)) {
                          insert_search_insight_SQL <- paste0(search_insight_SQL, insert_values[i], DUPLICATE_KEY_UPDATE_SQL)
                          dbSendQuery(connect_DB, insert_search_insight_SQL)
                        }
                      }
                    }
                  }
                }
              } else {
                # 伺服器端有問題
                error_message <- apple_search_ads_request %>% content %>% .$error %>% .$errors %>% `[[`(1) %>% 
                { paste(.$messageCode, " : ", .$message) }
                
                stop(paste(exe_datetime, apple_search_ads_request$status_code, error_message))
              }
              
              
              flog.info(paste0(exe_datetime, sprintf(' Apple search ads orgId: %s, campaign_id: %s, update completed !!', orgId, campaign_id)), name = project_name)
              print(sprintf(' Apple search ads orgId: %s, campaign_id: %s, update completed !!', orgId, campaign_id))
            } else {
              # ===== 6-1. 瀏覽campaign詳細數據=====
              print(sprintf(' Apple search ads orgId: %s, campaign_id: %s, 非搜尋campaign，為瀏覽campaign', orgId, campaign_id))
              # campaign_id變數跟欄位重複，重新命名
              campaign_idnum <- campaign_id
              
              # ===== 6-2. 瀏覽campaign資料匯入DB =====
              display_campaign <- campaign_data %>% filter(campaign_id == campaign_idnum) %>% select(-c('date','country_code','impressions','installs','taps','newDownloads','redownloads','localSpend.amount','adChannelType')) %>%
                dplyr::mutate(adgroup_id = '', adgroup_name = '', keyword_id = '', keyword_name = '', match_type = 'display') %>% unique()
              
              # 匯入資料庫
              if (nrow(display_campaign) > 0){
                flog.info(paste0(exe_datetime, sprintf(' 匯入ASA 瀏覽campaign資料，共%s筆。', nrow(display_campaign))), name = project_name)
                print(sprintf(' 匯入ASA 瀏覽campaign資料，共%s筆。', nrow(display_campaign)))
                
                # 將資料寫入(SQL版本)
                display_campaign_values <- display_campaign %>%
                  apply(., 1, function(x){
                    output <- x %>% 
                      paste0(., collapse = "', '") %>%
                      paste0("('", ., "')")
                    
                    return(output) }) %>%
                  paste0(., collapse = ",")
                # 將空格都排除，keyword_id等id是字串，數字進位後其它未進位的值會帶空白，是apply的原因
                display_campaign_values <- gsub("' ", "'", display_campaign_values, fixed = TRUE)
                
                display_campaign_SQL <- sprintf("INSERT mobile_game_asa_campaign (%s) VALUES ", 
                                               paste0(names(display_campaign), collapse = ", "))
                
                # ON DUPLICATE KEY UPDATE 組字串
                DUPLICATE_KEY_UPDATE_SQL <- names(display_campaign) %>% paste0(" = VALUES(",.,")") %>% 
                  paste0(names(display_campaign),.) %>%
                  paste0(collapse = " , ") %>% 
                  paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
                insert_display_campaign_SQL <- paste0(display_campaign_SQL, display_campaign_values, DUPLICATE_KEY_UPDATE_SQL)
                dbSendQuery(connect_DB, insert_display_campaign_SQL)
              }
              
              # ===== 6-3. 搜尋配對詳細資料匯入DB =====
              display_insight <- campaign_data %>% filter(campaign_id == campaign_idnum) %>% select(date, country_code, campaign_id, impressions, clicks = taps, installs, new_downloads = newDownloads, redownloads, cost = localSpend.amount) %>% 
                dplyr::mutate(adgroup_id = '', keyword_id = '')
              
              # 排除沒資料的數據
              display_insight %<>% filter(impressions != 0 | clicks != 0 | installs != 0 | cost != 0)
              
              # 匯入資料庫
              if (nrow(display_insight) > 0){
                flog.info(paste0(exe_datetime, sprintf(' 匯入ASA 瀏覽campaign詳細資料，共%s筆。', nrow(display_insight))), name = project_name)
                print(sprintf(' 匯入ASA 瀏覽campaign詳細資料，共%s筆。', nrow(display_insight)))
                
                # 資料分批寫入(SQL版本)
                insert_values <- NULL
                partition_seq <- seq(1, nrow(display_insight), by = partition_n)
                
                for (i in 1:length(partition_seq)) {
                  start_ind = partition_seq[i]
                  end_ind = partition_seq[i] - 1 + partition_n
                  
                  if (end_ind > nrow(display_insight)) {
                    end_ind = nrow(display_insight)
                  }
                  
                  insert_values[i] <- display_insight[start_ind:end_ind,] %>%
                    apply(., 1, function(x){
                      output <- x %>%
                        paste0(., collapse = "', '") %>%
                        paste0("('", ., "')")
                      
                      return(output) }) %>%
                    paste0(., collapse = ",")
                  
                  # 將空格都排除，keyword_id等id是字串，數字進位後其它未進位的值會帶空白，是apply的原因
                  insert_values[i] <- gsub("' ", "'", insert_values[i], fixed = TRUE)
                }
                
                display_insight_SQL <- sprintf("INSERT mobile_game_asa_insight (%s) VALUES ", paste0(names(display_insight), collapse = ", "))
                
                # ON DUPLICATE KEY UPDATE 組字串
                DUPLICATE_KEY_UPDATE_SQL <- names(display_insight) %>% paste0(" = VALUES(",.,")") %>% 
                  paste0(names(display_insight),.) %>%
                  paste0(collapse = " , ") %>% 
                  paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
                
                for (i in 1:length(insert_values)) {
                  insert_display_insight_SQL <- paste0(display_insight_SQL, insert_values[i], DUPLICATE_KEY_UPDATE_SQL)
                  dbSendQuery(connect_DB, insert_display_insight_SQL)
                }
              }
              
              flog.info(paste0(exe_datetime, sprintf(' Apple search ads orgId: %s, campaign_id: %s, update completed !!', orgId, campaign_id)), name = project_name)
              print(sprintf(' Apple search ads orgId: %s, campaign_id: %s, update completed !!', orgId, campaign_id))
            }
          }
        } else {
          flog.info(paste0(exe_datetime, sprintf(' Apple search ads orgId: %s, 沒有campaign資料 !!', orgId)), name = project_name)
          print(sprintf(' Apple search ads orgId: %s, 沒有campaign資料 !!', orgId))
        }
      } else {
        # 伺服器端有問題
        error_message <- apple_search_ads_request %>% content %>% .$error %>% .$errors %>% `[[`(1) %>% 
        { paste(.$messageCode, " : ", .$message) }
        stop(paste(exe_datetime, apple_search_ads_request$status_code, error_message))
      }
    }
    flog.info(paste0(exe_datetime, sprintf(' Apple search ads orgId: %s update completed !!', orgId)), name = project_name)
    print(sprintf(' Apple search ads orgId: %s update completed !!', orgId))
  } else {
    # 憑證失敗
    stop(paste(exe_datetime, "RCurl get_url 失敗"))
  }
  
  if (exists("connect_DB")){
    dbDisconnect(connect_DB)
  }
  
  flog.info(paste0(exe_datetime, " apple_search_ads_campaign Work End"), name = project_name)
  flog.info("=================================================================", name = project_name)
  
}, error = function(err){
  
  flog.error(paste0(exe_datetime, " apple_search_ads_campaign Error Fail: ",err), name = project_name)
  flog.info(paste0(exe_datetime, " apple_search_ads_campaign Work End"), name = project_name)
  
  # 寄信通知
  send.mail(from = all_config$mail$from,
            to = all_config$mail$to,
            subject = sprintf("apple_search_ads_campaign 異常 - %s", as.character(Sys.time())),
            body = sprintf("異常資訊：\n 程式執行失敗\n %s\n", 
                           err),
            encoding = "utf-8",
            smtp = list(host.name = "aspmx.l.google.com", port = 25),
            authenticate = FALSE,
            send = TRUE) 
  
  if (exists("connect_DB")){
    dbDisconnect(connect_DB)
  }
  
})
