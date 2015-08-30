#############################################################
# save csv to disk data
#############################################################

fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
  
  fn.init.worker("fm_01/build_fm")
  system(paste(
    "cd ../avito-context-click-py &&", 
    "pypy -u convert_csv_to_libffm.py", 
    "-input_files ../data/output-r/data.all.lr.csv",
    "-out_selector '{", 
    "\"../data/output-libffm/fm_01/data.val.tr.small.fm\": lambda file, row: row[\"SearchType\"] in [\"tr\"],",  
    "\"../data/output-libffm/fm_01/data.tr.tr.fm\": lambda file, row: row[\"SearchType\"] in [\"hist\"],",   
    "\"../data/output-libffm/fm_01/data.tr.tt.fm\": lambda file, row: row[\"SearchType\"] in [\"tr\"],",
    "\"../data/output-libffm/fm_01/data.val.tr.small.fm\": lambda file, row: row[\"SearchType\"] in [\"tr\"],",  
    "\"../data/output-libffm/fm_01/data.val.tr.fm\": lambda file, row: row[\"SearchType\"] in [\"hist\", \"tr\"],",   
    "\"../data/output-libffm/fm_01/data.val.tt.fm\": lambda file, row: row[\"SearchType\"] in [\"val\"],",
    "\"../data/output-libffm/fm_01/data.test.tr.small.fm\": lambda file, row: row[\"SearchType\"] in [\"tr\", \"val\"],", 
    "\"../data/output-libffm/fm_01/data.test.tr.fm\": lambda file, row: row[\"SearchType\"] in [\"hist\", \"tr\", \"val\"],", 
    "\"../data/output-libffm/fm_01/data.test.tt.fm\": lambda file, row: row[\"SearchType\"] in [\"test\"]",
    "}'",
    "-col_out IsClick",
    "-col_in_cat",
    "  AdCatID AdHistCTRBin AdID AdParams AdPriceBin AdTitleSZBin",
    "  Position",
    "  SearchAdCount SearchAdT1Count SearchAdT2Count SearchAdT3Count",
    "      SearchCatID SearchLocID SearchParamsSZBin SearchQuerySZBin SearchRussian",
    "  UserID UserIPID UserPrevQryDateBin UserQryTotalTimeBin",
    "-old_format",
     ">> ../data/log/fm_01/build_fm.log 2>&1"))
  fn.clean.worker()
  NULL
}
fn.kill.wk()

#############################################################
# train phase
#############################################################
fn.register.wk(2)
data.fm.01.pred.tmp <- foreach(test.type=c("tr", "val", "test"), .combine=rbind,
                               .noexport=all.noexport) %dopar% {
  
  log.name <- paste0("fm_01/fm_01_",test.type)
  fn.init.worker(log.name)
  
  system(paste(
    "../fm/fm",
    "-k 16 -t 20 -r 0.02 -s 6 -l 0.00001",
    paste0("../data/output-libffm/fm_01/data.",test.type,".tt.fm "),
    paste0("../data/output-libffm/fm_01/data.",test.type,".tr.fm "),
     " >> ", paste0("../data/log/",log.name,".log"), " 2>&1"))
  
  data.pred <- data.table(
    ID = data.all.lr.id[SearchType==test.type,ID],
    Pred = scan(paste0("../data/output-libffm/fm_01/data.",test.type,".tt.fm.out"))
  )
  fn.print.err(data.pred)
    
  fn.clean.worker()
  data.pred
}
fn.kill.wk()

data.fm.01.pred.tmp <- data.fm.01.pred.tmp[order(ID)]
Store(data.fm.01.pred.tmp)

data.fm.01.pred <- copy(data.fm.01.pred.tmp)

fn.print.err(data.fm.01.pred)
#      Size    Loss
# 1 7888752  0.04148 - tr
# 1 8512834  0.04334 - val
# 1 16401586 0.04244 - all

Store(data.fm.01.pred)

# fn.write.submission(data.fm.01.pred, "data.fm.01.pred")
