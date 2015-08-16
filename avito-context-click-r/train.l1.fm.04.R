#############################################################
# save csv to disk data
#############################################################

fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
  
  fn.init.worker("fm_04/build_fm")
  system(paste(
    "cd ../avito-context-click-py &&", 
    "pypy -u convert_csv_to_libffm.py", 
    "-input_files ../data/output-r/data.all.lr.csv",
    "-out_selector '{", 
    "\"../data/output-libffm/fm_04/data.tr.tr.fm\": lambda file, row: row[\"SearchType\"] in [\"hist\"],",   
    "\"../data/output-libffm/fm_04/data.tr.tt.fm\": lambda file, row: row[\"SearchType\"] in [\"tr\"],",
    "\"../data/output-libffm/fm_04/data.val.tr.fm\": lambda file, row: row[\"SearchType\"] in [\"hist\", \"tr\"],",   
    "\"../data/output-libffm/fm_04/data.val.tt.fm\": lambda file, row: row[\"SearchType\"] in [\"val\"],",
    "\"../data/output-libffm/fm_04/data.test.tr.fm\": lambda file, row: row[\"SearchType\"] in [\"hist\", \"tr\", \"val\"],", 
    "\"../data/output-libffm/fm_04/data.test.tt.fm\": lambda file, row: row[\"SearchType\"] in [\"test\"]",
    "}'",
    "-col_out IsClick",
    "-col_in_cat",
    "    UserAgentFamilyID UserAgentID UserAgentOSID UserDeviceID ",
    "     UserPrevPhoneRequest ",
    "     UserPrevPrevPrevQryDateBin UserPrevPrevQryDateBin ",
    "     UserPrevQryDateBin UserPrevVisitReq UserPrevVisitReqUni ",
    "     UserQryTotalTimeBin",
    "     Position",
    "-old_format",
    ">> ../data/log/fm_04/build_fm.log 2>&1"))
  fn.clean.worker()
  NULL
}
fn.kill.wk()

#############################################################
# train phase
#############################################################
fn.register.wk(1)
data.fm.04.pred.tmp <- foreach(test.type=c("val", "tr", "test"), .combine=rbind,
                               .noexport=all.noexport) %dopar% {
  
  log.name <- paste0("fm_04/fm_04_",test.type)
  fn.init.worker(log.name)
  
  system(paste(
    "ffm-train-predict",
    "-k 12 -t 5 -r 0.004 -s 12 -l 0.00001",
    paste0("../data/output-libffm/fm_04/data.",test.type,".tt.fm "),
    paste0("../data/output-libffm/fm_04/data.",test.type,".tr.fm "),
     " >> ", paste0("../data/log/",log.name,".log"), " 2>&1"))
  
  data.pred <- data.table(
    ID = data.all.lr.id[SearchType==test.type,ID],
    Pred = scan(paste0("../data/output-libffm/fm_04/data.",test.type,".tt.fm.out"))
  )
  fn.print.err(data.pred)
    
  fn.clean.worker()
  data.pred
}
fn.kill.wk()

data.fm.04.pred.tmp <- data.fm.04.pred.tmp[order(ID)]
Store(data.fm.04.pred.tmp)

data.fm.04.pred <- copy(data.fm.04.pred.tmp)

fn.print.err(data.fm.04.pred)
#      Size     Loss
# 1 7888752  0.04888 - tr
# 1 8512834  0.05135 - val
# 1 16401586 0.05017 - all

Store(data.fm.04.pred)

# fn.write.submission(data.fm.04.pred, "data.fm.04.pred")
