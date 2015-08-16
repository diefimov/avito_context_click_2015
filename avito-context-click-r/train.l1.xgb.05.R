#############################################################
# train model
#############################################################

wk.seed <- 5471887
fn.register.wk(1, seed=wk.seed)
data.l1.xgb.05.pred.tmp <- foreach(
  test.type=c("test"), .combine=rbind, .noexport=all.noexport) %dopar% {
  
  fn.init.new.fold.worker("l1_xgb_05", paste0(test.type, "2"))
  # fn.clean.worker()
  
  cat("\n\nSeed:", wk.seed, "...\n")
  

  data.tr <- xgb.DMatrix(fn.libsvm.file(
    paste0("data.", test.type, ".tr.full.libsvm")))
  data.test <- xgb.DMatrix(fn.libsvm.file(
    paste0("data.", test.type, ".tt.full.libsvm")))
  
  eval_metric = "logloss"
  data.watch = list(val=data.test)
  if (test.type == "val") {
    eval_metric <- function(preds, dtrain) {
      labels <- as.numeric(getinfo(dtrain, "label"))
      preds - as.numeric(preds)
      err <- round(fn.log.loss(actual=labels, pred=preds), digits=5)
      return(list(metric = "logloss", value = err))
    }
  } 
  
  data.fold$test.pred <- data.table(
    ID = sort(data.all.search.small[SearchType == test.type, ID]),
    Pred = 0.0,
    n = 0
  )
  
  fn.soar.unload(data.all.search.small)
  
  # Num rounds 66
  # Eta 0,5
  # Maxdepth 10
  # Colsample 0,375
  # Minchildweight 10

  data.fold$params = list(
    objective = "binary:logistic",
    eval_metric = eval_metric,
    nthread = 6,
    eta = 0.18,
    max_depth = 10,
    gamma = 0.8, 
    colsample_bytree = 0.7, 
    min_child_weight = 5,
    colsample_bylevel = 0.8
  )
  
  data.fold$nrounds <- 75
  
  cat("\nParams:\n")
  print(data.fold$params)
  
  n.models <- 10
  for (ix in 1:n.models) {
    
    cat("\n\nTraining ", ix, "of", n.models,"...\n")
    
    set.seed(ix + 89475560)
    
    model =  xgb.train(
      data = data.tr,
      watchlist=data.watch,
      params = data.fold$params,
      nrounds = data.fold$nrounds,
      verbose = 1)
    
    
    ntreelimit <- data.fold$nrounds
    try.pred <- T
    
    while (try.pred) {
      pred.cur <- xgboost::predict(model, data.test, ntreelimit=ntreelimit)
      pred.cur.avg <- mean(pred.cur)
      
      cat("\nCurrent prediction avg of", length(pred.cur),
          "instances:", pred.cur.avg, "\n")
      if (test.type == "val" || 
          (pred.cur.avg >= 0.006 && pred.cur.avg <= 0.016)) {
        
        try.pred <- F
        data.fold$test.pred[ , Pred := (Pred*n + pred.cur)/(n+1)]
        data.fold$test.pred[,  n := n+1]

        
        fn.save.data.fold(data.fold)
        cat("\nPrediction with", ntreelimit ,"trees included\n")
      } else {
        cat("\nPrediction with", ntreelimit ,"trees discarded\n")
        ntreelimit <- ntreelimit - 5
        try.pred <- ntreelimit >= 60
      }
    }

    cat("\nPrediction status using", data.fold$test.pred$n[1], "iteration(s) :\n")
    fn.print.err(data.fold$test.pred)

    set.seed(Sys.time())
    rm(pred.cur, pred.cur.avg)
    invisible(gc())
    
  }

#   cat("\n\nFeature importance:\n") 
#   data.fold$importance <- xgb.importance(
#     feature_names=cols.in.combine, model=model)
#   print(data.fold$importance)
  
  fn.clean.worker()
  
  data.fold$test.pred
  
}
fn.kill.wk()

data.l1.xgb.05.pred.tmp <- data.l1.xgb.05.pred.tmp[order(ID)]
Store(data.l1.xgb.05.pred.tmp)

for (ix in "2") {
  test.type <- "test"
  cat("\nLoading",test.type, ix,"...\n")
  
  fn.init.fold.worker("l1_xgb_05", paste0(test.type, ix), no.log=T)
  pred.nam <- paste("data.l1.xgb.05.pred", test.type, ix, sep=".")
  assign(pred.nam, data.fold$test.pred[order(ID)])
  cat("Saving",pred.nam,"...\n")
  Store(list=pred.nam)
}

data.l1.xgb.05.pred.tmp <- rbind(
  data.l1.xgb.05.pred.test.2
)[order(ID), list(Pred=sum(Pred*n)/sum(n)), by="ID"]


data.l1.xgb.05.pred <- copy(data.l1.xgb.05.pred.tmp)

#############################################################
# save data
#############################################################

# fn.print.err(data.l1.xgb.05.pred)


Store(data.l1.xgb.05.pred) #  0.04076

cat('Test avg:', mean(data.l1.xgb.05.pred[ID > 0]$Pred), "\n") 
# Test avg: 0.007848369

# fn.write.submission(data.l1.xgb.05.pred, "data.l1.xgb.05.pred")


