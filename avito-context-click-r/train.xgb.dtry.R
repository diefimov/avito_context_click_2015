load(fn.rdata.file("data.reduced.all.RData"))
flist <- setdiff(colnames(data.reduced.all), c("SearchType", "SearchDayYear"))

write.table(as.data.frame(data.reduced.all[SearchType==1][,flist,with=F]),
            file = fn.out.file("train.xgb.csv"),
            quote = F,
            sep = ",",
            row.names = F,
            col.names = T)

write.table(as.data.frame(data.reduced.all[SearchType==2][,flist,with=F]),
            file = fn.out.file("val.xgb.csv"),
            quote = F,
            sep = ",",
            row.names = F,
            col.names = T)

write.table(as.data.frame(data.reduced.all[SearchType==3][,flist,with=F]),
            file = fn.out.file("test.xgb.csv"),
            quote = F,
            sep = ",",
            row.names = F,
            col.names = T)

write.table(as.data.frame(data.reduced.all[SearchType==1 | SearchType==2][,flist,with=F]),
            file = fn.out.file("train.val.xgb.csv"),
            quote = F,
            sep = ",",
            row.names = F,
            col.names = T)

n <- nrow(data.reduced.all[SearchType==1])
set.seed(23243)
ix.train <- sample(c(1:n), 0.2*n)
n <- nrow(data.reduced.all[SearchType==2])
set.seed(102903)
ix.val <- sample(c(1:n), 0.2*n)

write.table(as.data.frame(data.reduced.all[SearchType==1][ix.train][,flist,with=F]),
            file = fn.out.file("train.part.xgb.csv"),
            quote = F,
            sep = ",",
            row.names = F,
            col.names = T)

write.table(as.data.frame(data.reduced.all[SearchType==2][ix.val][,flist,with=F]),
            file = fn.out.file("val.part.xgb.csv"),
            quote = F,
            sep = ",",
            row.names = F,
            col.names = T)
val.part.actual <- data.reduced.all[SearchType==2][ix.val][,list(ID,IsClick)]
setkey(val.part.actual, ID)
rm(data.reduced.all)
gc()


### lb part ###
system(paste("cd ../avito-context-click-py && python train_xgb_dtry.py",
             "--train", fn.out.file("train.val.xgb.csv"),
             "--test", fn.out.file("test.xgb.csv"),
             "--pred", fn.py.file("test.pred.xgb.csv"),
             "--epoch", 15,
             ">> ../data/log/xgb.dtry.log 2>&1"))  

test.pred <- list()
for (i in c(0:14)) {
  test.pred[[length(test.pred)+1]] <- fread(fn.py.file(paste0("test.pred.xgb.epoch",i,".csv")))
  cat(mean(test.pred[[length(test.pred)]]$IsClick), "...\n")
}
test.pred <- rbindlist(test.pred, use.names=T, fill=F)
test.pred <- test.pred[,list(IsClick = round(mean(IsClick), 6)), by="ID"]

write.table(as.data.frame(test.pred),
            file = fn.submission.file("dtry.xgb9__0.0409xx"),
            quote = F,
            sep = ",",
            row.names = F,
            col.names = T)


### cv part ###
#system(paste("cd ../avito-context-click-py && python train_xgb_dtry.py",
#             "--train", fn.out.file("train.part.xgb.csv"),
#             "--test", fn.out.file("val.part.xgb.csv"),
#             "--pred", fn.py.file("val.part.pred.xgb.csv"),
#             "--epoch", 15,
#             ">> ../data/log/xgb.dtry.log 2>&1"))  

#val.part.pred <- list()
#for (i in c(0:4)) {
#  val.part.pred[[length(val.part.pred)+1]] <- fread(fn.py.file(paste0("val.part.pred.xgb.epoch",i,".csv")))
#  cat(mean(val.part.pred[[length(val.part.pred)]]$IsClick), "...\n")
#}
#val.part.pred <- rbindlist(val.part.pred, use.names=T, fill=F)
#val.part.pred <- val.part.pred[,list(IsClick = round(mean(IsClick), 6)), by="ID"]
#setkey(val.part.pred, ID)
#fn.logloss(val.part.actual$IsClick, val.part.pred$IsClick)
