#############################################################
# save csv to disk data
#############################################################

fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
  
  fn.init.worker("fm_05/build_fm")
  system(paste(
    "cd ../avito-context-click-py &&", 
    "pypy -u convert_csv_to_libffm.py", 
    "-input_files ../data/output-r/data.all.lr.csv",
    "-out_selector '{", 
    "\"../data/output-libffm/fm_05/data.tr.tr.fm\": lambda file, row: row[\"SearchType\"] in [\"hist\"],",   
    "\"../data/output-libffm/fm_05/data.tr.tt.fm\": lambda file, row: row[\"SearchType\"] in [\"tr\"],",
    "\"../data/output-libffm/fm_05/data.val.tr.fm\": lambda file, row: row[\"SearchType\"] in [\"hist\", \"tr\"],",   
    "\"../data/output-libffm/fm_05/data.val.tt.fm\": lambda file, row: row[\"SearchType\"] in [\"val\"],",
    "\"../data/output-libffm/fm_05/data.test.tr.fm\": lambda file, row: row[\"SearchType\"] in [\"hist\", \"tr\", \"val\"],", 
    "\"../data/output-libffm/fm_05/data.test.tt.fm\": lambda file, row: row[\"SearchType\"] in [\"test\"]",
    "}'",
    "-col_out IsClick",
    "-col_in_cat",
    "    CountAdSearchBin CountAdSearchCatBin CountAdSearchLocBin ",
    "     CountAdUsersBin CountIPUserBin CountUserAdBin ",
    "     CountUserAdDupT1Bin CountUserAdDupT3Bin CountUserAdT1Bin ",
    "     CountUserAdT3Bin CountUserSearchBin CountUserSearchCategoryBin ",
    "     CountUserSearchLocationBin ",
    "     RatioAdPos1Bin RatioSearchRussBin ",
    "   Position",
    "-old_format",
    ">> ../data/log/fm_05/build_fm.log 2>&1"))
  fn.clean.worker()
  NULL
}
fn.kill.wk()

#############################################################
# train phase
#############################################################
fn.register.wk(1) #  
data.fm.05.pred.tmp <- foreach(test.type=c("val" , "tr", "test"), .combine=rbind,
                               .noexport=all.noexport) %dopar% {
  
  log.name <- paste0("fm_05/fm_05_",test.type)
  fn.init.worker(log.name)
  
  system(paste(
    "ffm-train-predict",
    "-k 12 -t 5 -r 0.004 -s 12 -l 0.00001",
    paste0("../data/output-libffm/fm_05/data.",test.type,".tt.fm "),
    paste0("../data/output-libffm/fm_05/data.",test.type,".tr.fm "),
     " >> ", paste0("../data/log/",log.name,".log"), " 2>&1"))
  
  data.pred <- data.table(
    ID = data.all.lr.id[SearchType==test.type,ID],
    Pred = scan(paste0("../data/output-libffm/fm_05/data.",test.type,".tt.fm.out"))
  )
  fn.print.err(data.pred)
    
  fn.clean.worker()
  data.pred
}
fn.kill.wk()

data.fm.05.pred.tmp <- data.fm.05.pred.tmp[order(ID)]
Store(data.fm.05.pred.tmp)

data.fm.05.pred <- copy(data.fm.05.pred.tmp)

fn.print.err(data.fm.05.pred)
#      Size     Loss
# 1 7888752  0.04992 - tr
# 1 8512834  0.04812 - val
# 1 16401586 0.04905 - all

Store(data.fm.05.pred)

# fn.write.submission(data.fm.05.pred, "data.fm.05.pred")
