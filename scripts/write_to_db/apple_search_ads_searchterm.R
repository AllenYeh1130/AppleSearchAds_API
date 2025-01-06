# ===
# 從Apple Search Ads 獲得 keywrod的searchterm 資訊，並匯入資料庫
project_name <- 'apple_search_ads_searchterm'
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
  flog.info(paste0(exe_datetime, " Work start apple_search_ads_searchterm"), name = project_name)
  
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
    # 確定多天數要執行的迴圈
    ori_start_date <- start_date
    date_loop <- ceiling((unclass(end_date-start_date)[1]+1))  
    
    for (p in 1:date_loop) {
      # 判定每次回圈的日期
      # p <- 1
      start_date <- ori_start_date +p -1
      end_date <- start_date
    
      # ===== 2. 查詢各廣告帳號 =====
      # orgId <- '123456'
      for (orgId in orgId_list) {
        flog.info(paste0(exe_datetime, sprintf(' Apple search ads searchterm update start !! start_date: %s, end_date: %s, orgId: %s', start_date, end_date, orgId)), name = project_name)
        print(sprintf(' Apple search ads searchterm update start !! start_date: %s, end_date: %s, orgId: %s', start_date, end_date, orgId))
        
        # ===== 3. 確認campaign數據 ===== 
        api_url <- sprintf("https://api.searchads.apple.com/api/%s/reports/campaigns", asa_config$apple_search_ads$api_version)
        granularity <- 'DAILY'
        groupBy <- '["countryOrRegion"]'
        selector_orderBy <- '[{"field":"impressions","sortOrder":"DESCENDING"}]'
        selector_pagination <- sprintf('{"offset":0,"limit": 1000}')
        report_body <- sprintf('{"startTime": "%s", "endTime": "%s",
                               "granularity": "%s", "groupBy": %s,
                               "selector": {"orderBy": %s, "pagination": %s}}',
                               start_date, end_date, granularity, groupBy,
                               selector_orderBy, selector_pagination)
        
        apple_search_ads_request <- httr::POST(api_url, 
                                               body = report_body,
                                               add_headers(
                                                 Authorization = api_Authorization,
                                                 `X-AP-Context` = sprintf("orgId=%s", orgId),
                                                 `Content-Type` = "application/json"),
                                               timeout(180), 
                                               verbose())
        Sys.sleep(2)
        
        # 判斷伺服器回應
        if (apple_search_ads_request$status_code == 200){
          # 轉換JSON
          campaign_json_data  <- apple_search_ads_request$content %>% rawToChar %>% jsonlite::fromJSON(flatten = TRUE) %>%
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
              flog.info(paste0(exe_datetime, sprintf(' Apple search ads searchterm orgId: %s, campaign_id: %s, update start !!', orgId, campaign_id)), name = project_name)
              print(sprintf(' Apple search ads searchterm orgId: %s, campaign_id: %s, update start !!', orgId, campaign_id))
              
              # ===== 5. 辨認搜尋campaign，Search terms只在搜尋campiagn才有 =====
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
                  
                  # ===== 5-1. 搜尋campaign詳細數據 (adgroup_id, keyword_id) =====
                  api_url <- sprintf("https://api.searchads.apple.com/api/%s/reports/campaigns/%s/searchterms", asa_config$apple_search_ads$api_version, campaign_id)
                  granularity <- 'DAILY'
                  groupBy <- '["countryOrRegion"]'
                  selector_orderBy <- '[{"field":"impressions","sortOrder":"DESCENDING"}]'
                  selector_pagination <- sprintf('{"offset":%s,"limit":%s}', offset, pagination_limit)
                  report_body <- sprintf('{"startTime": "%s", "endTime": "%s",
                                         "groupBy": %s,
                                         "returnRecordsWithNoMetrics": "false",
                                         "returnRowTotals": true,
                                         "returnGrandTotals": true,
                                         "selector": {"orderBy": %s, "pagination": %s}}',
                                         start_date, end_date, groupBy,
                                         selector_orderBy, selector_pagination)
                  
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
                    searchterm_json_data  <- apple_search_ads_request$content %>% rawToChar %>% jsonlite::fromJSON(flatten = TRUE) %>%
                      .$data %>% .$reportingDataResponse %>% .$row
                    
                    # ===== 5-2. 檢查是否有資料，取得時間內各campaign的searchterm資訊 =====
                    if (is.data.frame(searchterm_json_data)){
                      # 檢查資料欄位
                      names(searchterm_json_data) %<>% str_replace(., "total.", "")
                      names(searchterm_json_data) %<>% str_replace(., "metadata.", "")
                      searchterm_data <- data.frame(NULL, stringsAsFactors = FALSE)
                      searchterm_data <- searchterm_json_data %>% apply(1, function(x){
                        # searchterm沒有數據的欄位會直接沒有，需要強制建立欄位避免少欄位
                        searchterm_data %<>% plyr::rbind.fill(data.frame(impressions=0, taps=0, installs=0, newDownloads=0, redownloads=0, localSpend.amount=0))
                        searchterm_data$impressions <- ifelse(is.na(x['impressions']), 0, x['impressions'])
                        searchterm_data$taps <- ifelse(is.na(x['taps']), 0, x['taps'])
                        searchterm_data$installs <- ifelse(is.na(x['installs']), 0, x['installs'])
                        searchterm_data$newDownloads <- ifelse(is.na(x['newDownloads']), 0, x['newDownloads'])
                        searchterm_data$redownloads <- ifelse(is.na(x['redownloads']), 0, x['redownloads'])
                        searchterm_data$localSpend.amount <- ifelse(is.na(x['localSpend.amount']), 0, x['localSpend.amount'])
                        searchterm_data$adgroup_id <- x['adGroupId']
                        searchterm_data$keyword_id <- x['keywordId']
                        searchterm_data$country_code <- ifelse(is.na(x['countryOrRegion']), "unknown", x['countryOrRegion'])
                        searchterm_data$search_term <- x['searchTermText']
                        # 搜尋配對沒有keyword_id帶空白
                        searchterm_data$keyword_id[is.na(searchterm_data$keyword_id)] <- ""
                        # serach_term沒有名稱，將NA替代為字串(無名稱)
                        searchterm_data$search_term[is.na(searchterm_data$search_term)] <- "(不顯示名稱)"
                        # 沒有資料的數值帶0
                        searchterm_data[is.na(searchterm_data)] <- 0
                        searchterm_data %<>% as.data.frame()
                        return(searchterm_data)
                      }) %>% do.call(plyr::rbind.fill, .) %>% dplyr::mutate(campaign_id = as.integer(campaign_id), date = start_date) %>% 
                      select(date, campaign_id, adgroup_id, keyword_id, country_code, search_term, impressions, taps, installs, newDownloads, redownloads, localSpend.amount)
                      
                      # 排除單引號的字元，避免組SQL出錯
                      searchterm_data$search_term <- gsub("'", "", searchterm_data$search_term)
                      
                      # 排除沒資料的數據
                      searchterm_data %<>% filter(impressions != 0 | taps != 0 | installs != 0 | localSpend.amount != 0)
                      
                      # ===== 5-3. 詳細searchterm資料匯入DB =====
                      # 根據searchterm詳細資訊調整名稱跟欄位
                      searchterm_insight <- searchterm_data %>% inner_join(campaign %>% select(campaign_id), by = 'campaign_id') %>%
                        rename(clicks = taps, new_downloads = newDownloads, cost = localSpend.amount)
                      
                      # 匯入資料庫
                      if (nrow(searchterm_insight) > 0){
                        flog.info(paste0(exe_datetime, sprintf('匯入ASA searchterm詳細資料，共%s筆。', nrow(searchterm_insight))), name = project_name)
                        print(sprintf('匯入ASA searchterm詳細資料，共%s筆。', nrow(searchterm_insight)))
                        
                        # 資料分批寫入(SQL版本)
                        insert_values <- NULL
                        partition_seq <- seq(1, nrow(searchterm_insight), by = partition_n)
                        
                        for (i in 1:length(partition_seq)) {
                          start_ind = partition_seq[i]
                          end_ind = partition_seq[i] - 1 + partition_n
                          
                          if (end_ind > nrow(searchterm_insight)) {
                            end_ind = nrow(searchterm_insight)
                          }
                          
                          insert_values[i] <- searchterm_insight[start_ind:end_ind,] %>%
                            apply(., 1, function(x){
                              output <- x %>%
                                paste0(., collapse = "', '") %>%
                                paste0("('", ., "')")
                              
                              return(output) }) %>%
                            paste0(., collapse = ",")
                          
                          # 將空格都排除，keyword_id等id是字串，數字進位後其它未進位的值會帶空白，是apply的原因
                          insert_values[i] <- gsub("' ", "'", insert_values[i], fixed = TRUE)
                        }
                        searchterm_insight_SQL <- sprintf("INSERT mobile_game_asa_searchterm (%s) VALUES ", 
                                                   paste0(names(searchterm_insight), collapse = ", "))
                        
                        # ON DUPLICATE KEY UPDATE 組字串
                        DUPLICATE_KEY_UPDATE_SQL <- names(searchterm_insight) %>% paste0(" = VALUES(",.,")") %>% 
                          paste0(names(searchterm_insight),.) %>%
                          paste0(collapse = " , ") %>% 
                          paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
                        
                        for (i in 1:length(insert_values)) {
                          insert_searchterm_insight_SQL <- paste0(searchterm_insight_SQL, insert_values[i], DUPLICATE_KEY_UPDATE_SQL)
                          dbSendQuery(connect_DB, insert_searchterm_insight_SQL)
                        }
                      }
                    }
                    
                    # ===== 5-4. 確認是否有分頁 =====
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
                  flog.info(paste0(exe_datetime, sprintf(' Apple search ads searchterm orgId: %s, campaign_id: %s, update completed !!', orgId, campaign_id)), name = project_name)
                  print(sprintf(' Apple search ads searchterm orgId: %s, campaign_id: %s, update completed !!', orgId, campaign_id))
                }
              }
            }
          } else {
            flog.info(paste0(exe_datetime, sprintf(' Apple search ads searchterm orgId: %s, 沒有campaign資料 !!', orgId)), name = project_name)
            print(sprintf(' Apple search ads searchterm orgId: %s, 沒有campaign資料 !!', orgId))
          }
        } else {
          # 伺服器端有問題
          error_message <- apple_search_ads_request %>% content %>% .$error %>% .$errors %>% `[[`(1) %>% 
          { paste(.$messageCode, " : ", .$message) }
          stop(paste(exe_datetime, apple_search_ads_request$status_code, error_message))
        }
      }
      flog.info(paste0(exe_datetime, sprintf(' Apple search ads searchterm orgId: %s update completed !!', orgId)), name = project_name)
      print(sprintf(' Apple search ads searchterm orgId: %s update completed !!', orgId))
    }
  } else {
    # 憑證失敗
    stop(paste(exe_datetime, "RCurl get_url 失敗"))
  }
  
  if (exists("connect_DB")){
    dbDisconnect(connect_DB)
  }
  
  flog.info(paste0(exe_datetime, " apple_search_ads_searchterm Work End"), name = project_name)
  flog.info("=================================================================", name = project_name)
  
}, error = function(err){
  
  flog.error(paste0(exe_datetime, " apple_search_ads_searchterm Error Fail: ",err), name = project_name)
  flog.info(paste0(exe_datetime, " apple_search_ads_searchterm Work End"), name = project_name)
  
  # 寄信通知
  send.mail(from = mysql_data$mail$from,
            to = mysql_data$mail$to,
            subject = sprintf("apple_search_ads_searchterm 異常 - %s", as.character(Sys.time())),
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
