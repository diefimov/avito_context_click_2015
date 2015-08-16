##############################################################
## load data
##############################################################
tic()
cat("Loading csv data... \n")

fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
  
  fn.init.worker('data_build/load_data')
  
  data.all.search.info <- fread(fn.in.file(search.info.file))
  data.all.search.info[, SearchDate := as.numeric(fn.parse.date(SearchDate)) ]
  data.all.search.info <- data.all.search.info[order(SearchID)]
  
  russian.alphabet <- c("а|б|в|г|д|е|ж|з|и|й|к|л|м|н|о|п|р|с|т|у|ф|х|ц|ч|ш|щ|ъ|ы|ь|э|ю|я")
  data.all.search.info[, SearchRussian := 
                         as.numeric(SearchQuery %like% russian.alphabet)]

  data.all.search.info[, SearchParamsSZ := nchar(SearchParams)]
  data.all.search.info[, SearchQuerySZ := nchar(SearchQuery)]
  
  param.breaks <- sort(unique(c(-1, 0, 1, 3, 10, 20, Inf)))
  data.all.search.info[, SearchParamsSZBin := 
                         cut(SearchQuerySZ, labels=F, breaks=param.breaks)]
  
  qry.breaks <- sort(unique(c(-1, 0, 1, 2, 3, 10, 20, 35, 1000)))
  data.all.search.info[, SearchQuerySZBin := 
                         cut(SearchQuerySZ, labels=F, breaks=qry.breaks)]
  
  data.all.search.info.disc <- data.all.search.info[
    , list(SearchID, SearchParamsSZBin, SearchQuerySZBin)]
  
  data.all.search.info.cont <- data.all.search.info[
    , list(SearchID, SearchParamsSZ, SearchQuerySZ)]
  
  data.all.search.info[, SearchParams := NULL]
  data.all.search.info[, SearchQuery := NULL]
  data.all.search.info[, SearchParamsSZ := NULL]
  data.all.search.info[, SearchQuerySZ := NULL]
  data.all.search.info[, SearchParamsSZBin := NULL]
  data.all.search.info[, SearchQuerySZBin := NULL]
  
  data.all.search.ip.count <- data.all.search.info[
    ,list(CountIPUser=length(unique(UserID))), by="IPID"]
  ipuser.breaks <- sort(unique(c(-Inf, 1, 3, 10, Inf)))
  data.all.search.ip.count[, CountIPUserBin := 
                         cut(CountIPUser, labels=F, breaks=ipuser.breaks)]
  
  data.all.search.ip.count <- merge(
    data.all.search.info[,list(SearchID, IPID)], 
    data.all.search.ip.count,
    by="IPID")[order(SearchID)]
  
  fn.check.searchid(data.all.search.ip.count, 
                    data.all.search.info.cont,
                    data.all.search.info.disc)
  
  data.all.search.info.cont[
    , CountIPUser := data.all.search.ip.count$CountIPUser]
  data.all.search.info.disc[
    , CountIPUserBin := data.all.search.ip.count$CountIPUserBin]
  
  rm(data.all.search.ip.count)
  
  data.all.search.user.count <- data.all.search.info[
    ,list(
      CountUserSearchLocation=length(unique(LocationID)),
      CountUserSearchCategory=length(unique(CategoryID)),
      CountUserSearch=length(unique(SearchID))
      ), by="UserID"]
  
  user.loc.breaks <- sort(unique(c(-Inf, 1, 2, 3, 5, Inf)))
  data.all.search.user.count[, CountUserSearchLocationBin := 
                             cut(CountUserSearchLocation, labels=F, 
                                 breaks=user.loc.breaks)]
  
  user.cat.breaks <- sort(unique(c(-Inf, 1, 2, 3, 7, Inf)))
  data.all.search.user.count[, CountUserSearchCategoryBin := 
                               cut(CountUserSearchCategory, labels=F, 
                                   breaks=user.cat.breaks)]
  
  user.srch.breaks <- sort(unique(c(-Inf, 1, 2, 3, 10, 30, 100, Inf)))
  data.all.search.user.count[, CountUserSearchBin := 
                               cut(CountUserSearch, labels=F, 
                                   breaks=user.srch.breaks)]
  
  data.all.search.user.count <- merge(
    data.all.search.info[,list(SearchID, UserID)], 
    data.all.search.user.count,
    by="UserID")[order(SearchID)]
  
  fn.check.searchid(data.all.search.user.count, 
                    data.all.search.info.cont,
                    data.all.search.info.disc)
  
  for (col.nam in c("CountUserSearchLocation", "CountUserSearchCategory",
                    "CountUserSearch")){
    data.all.search.info.cont[
      , col.nam := data.all.search.user.count[[col.nam]], with=F]
    col.nam.bin <- paste0(col.nam,"Bin")
    data.all.search.info.disc[
      , col.nam.bin := data.all.search.user.count[[col.nam.bin]], with=F]
  }
  
  
  rm(data.all.search.user.count)

  Store(data.all.search.info, data.all.search.info, data.all.search.info.cont,
        data.all.search.info.disc)
  invisible(gc(T))

  data.tr <- fread(fn.in.file(train.file))
  data.tr <- data.tr[, ID := -(SearchID*10 + Position)]
  setcolorder(data.tr, c("ID", "SearchID", "IsClick", "AdID", "Position", 
                         "HistCTR", "ObjectType"))
  
  data.test <- fread(fn.in.file(test.file))
  data.test[, IsClick := NA_integer_]
  
  data.all.search.full <- rbind(
    data.tr, 
    data.test
  )[order(ID)]
  
  rm(data.tr, data.test)
  invisible(gc(T))
  
  data.all.search <- data.all.search.full[ObjectType == 3]
  data.all.search[, ObjectType := NULL]
  ctr.breaks <- sort(unique(c(seq(0,0.05,0.005), seq(0.05,0.1,0.01), 
                              0.2, 0.3, 0.5, 1)))
  setnames(data.all.search, "HistCTR", "AdHistCTR")
  data.all.search[, AdHistCTRBin := cut(AdHistCTR, labels=F, breaks=ctr.breaks)]
  
  data.all.search.cont <- data.all.search[, list(ID, AdHistCTR)]
  data.all.search.disc <- data.all.search[, list(ID, AdHistCTRBin)]
  
  data.all.search[, AdHistCTR := NULL]
  data.all.search[, AdHistCTRBin := NULL]

  fn.check.id(data.all.search, data.all.search.cont, data.all.search.disc)
  
  Store(data.all.search, data.all.search.cont, data.all.search.disc)
  invisible(gc(T))
  
  data.all.search.full[, HistCTR := NULL]
  
  Store(data.all.search.full)
  invisible(gc(T))
  
  
  data.all.search.count <- data.all.search.full[
    , list(SearchAdCount=.N, 
           SearchAdT1Count=sum(ObjectType==1),
           SearchAdT2Count=sum(ObjectType==2),
           SearchAdT3Count=sum(ObjectType==3))
    , by="SearchID"]
  fn.soar.unload(data.all.search.full)
  invisible(gc(T))
  
  data.all.search.info <- merge(data.all.search.info, 
                                data.all.search.count,
                                by="SearchID")[order(SearchID)]
  rm(data.all.search.count)
  
  fn.check.searchid(data.all.search.info)
  Store(data.all.search.info)
  invisible(gc(T))
  
  
  data.user.ad.info <- merge(
    data.all.search.full, data.all.search.info[ , list(SearchID, UserID)],
    by="SearchID")
  fn.soar.unload(data.all.search.full, data.all.search.info)
  
  data.user.ad.info <- data.user.ad.info[
    ,list(
      CountUserAd = length(unique(AdID)),
      CountUserAdT1 = length(unique(AdID[ObjectType==1])),
      CountUserAdT3 = length(unique(AdID[ObjectType==3])),
      CountUserAdDupT1 = sum(duplicated(AdID[ObjectType==1])),
      CountUserAdDupT3 = sum(duplicated(AdID[ObjectType==3]))
    ), by="UserID"
  ]
  
  user.ad.break <- c(0,1,3,10,30,100,Inf)
  data.user.ad.info[, CountUserAdBin := cut(CountUserAd, labels=F, 
                                            breaks=user.ad.break)]
  user.adt1.break <- c(-1,0,1,3,10,30,100,Inf)
  data.user.ad.info[, CountUserAdT1Bin := cut(CountUserAdT1, labels=F, 
                                            breaks=user.adt1.break)]
  user.adt3.break <- c(-1,0,1,3,10,30,100,Inf)
  data.user.ad.info[, CountUserAdT3Bin := cut(CountUserAdT3, labels=F, 
                                              breaks=user.adt3.break)]
  
  user.adt1.dup.break <- c(-1,0,10,30,100,Inf)
  data.user.ad.info[, CountUserAdDupT1Bin := cut(CountUserAdDupT1, labels=F, 
                                              breaks=user.adt1.dup.break)]
  
  user.adt3.dup.break <- c(-1,0,3,30,100,Inf)
  data.user.ad.info[, CountUserAdDupT3Bin := cut(CountUserAdDupT3, labels=F, 
                                                 breaks=user.adt3.dup.break)]
  
  data.user.ad.info <- merge(
    data.all.search.info[, list(SearchID, UserID)], 
    data.user.ad.info,
    by="UserID")[order(SearchID)]
  
  fn.check.searchid(data.user.ad.info, data.all.search.info.cont, 
                    data.all.search.info.disc)
  
  for (col.nam in c("CountUserAd", "CountUserAdT1", "CountUserAdT3",
                    "CountUserAdDupT1", "CountUserAdDupT3")){
    data.all.search.info.cont[
      , col.nam := data.user.ad.info[[col.nam]], with=F]
    col.nam.bin <- paste0(col.nam,"Bin")
    data.all.search.info.disc[
      , col.nam.bin := data.user.ad.info[[col.nam.bin]], with=F]
  }
  
  rm(data.user.ad.info)
  Store(data.all.search.info.cont, data.all.search.info.disc)
  invisible(gc(T))
  
  data.ad.info <- merge(data.all.search, data.all.search.info, by="SearchID")
  fn.soar.unload(data.all.search, data.all.search.info)
  
  data.ad.info <- data.ad.info[
    ,list(
      ID=ID,
      CountAdSearch = length(unique(SearchID)),
      RatioAdPos1 = sum(Position==1)/.N,
      CountAdUsers = length(unique(UserID)),
      CountAdSearchLoc = length(unique(LocationID)),
      CountAdSearchCat = length(unique(CategoryID)),
      RatioSearchRuss = sum(SearchRussian==1)/.N
    ), by="AdID"
  ][order(ID)]
  data.ad.info[, AdID:=NULL]
  
  cur.break <- c(0, 10^seq(2,8,0.5))
  data.ad.info[, CountAdSearchBin := 
                 cut(CountAdSearch, labels=F, breaks=cur.break)]
  
  cur.break <- c(-Inf,seq(0,1,0.1))
  data.ad.info[, RatioAdPos1Bin := 
                 cut(RatioAdPos1, labels=F, breaks=cur.break)] 
  
  cur.break <- c(0, 10^seq(2,8,0.5))
  data.ad.info[, CountAdUsersBin := 
                 cut(CountAdUsers, breaks=cur.break, labels=F)]
  
  cur.break <- c(0, 10^seq(0.5,4,0.25))
  data.ad.info[, CountAdSearchLocBin := 
                 cut(CountAdSearchLoc, breaks=cur.break, labels=F)]
  
  cur.break <- c(0:5)
  data.ad.info[, CountAdSearchCatBin := 
                 cut(CountAdSearchCat, breaks=cur.break, labels=F)]
  
  cur.break <- c(-Inf,seq(0,1,0.1))
  data.ad.info[, RatioSearchRussBin := 
                 cut(RatioSearchRuss, labels=F, breaks=cur.break)] 
  
  fn.check.id(data.ad.info, data.all.search.cont, data.all.search.disc)
  
  for (col.nam in setdiff(colnames(data.ad.info), "ID")){
    if (col.nam %like% 'Bin$') {
      data.all.search.disc[
      , col.nam := data.ad.info[[col.nam]], with=F]
    } else {
      data.all.search.cont[
      , col.nam := data.ad.info[[col.nam]], with=F]
    }
  }
  rm(data.ad.info)
  invisible(gc(T))
  Store(data.all.search.cont, data.all.search.disc)
  invisible(gc(T))
  
  
  fn.shift.itvl <- function(x, n=length(x)) c(NA_real_, x[-n])
  fn.prev.itvl <- function(x, n=length(x)) x - fn.shift.itvl(x)
  
  data.all.search.interval <- data.all.search.info[
    order(SearchDate),
    list(
      SearchID=SearchID,
      UserQryTotalTime=SearchDate-min(SearchDate),
      UserPrevQryDate = fn.prev.itvl(x=SearchDate),
      UserPrevPrevQryDate = fn.shift.itvl(fn.prev.itvl(x=SearchDate)) + 
        fn.prev.itvl(x=SearchDate),
      UserPrevPrevPrevQryDate = fn.shift.itvl(fn.shift.itvl(
        fn.prev.itvl(x=SearchDate))) + fn.shift.itvl(fn.prev.itvl(x=SearchDate)) + 
        fn.prev.itvl(x=SearchDate)
    ), by="UserID"
    ][order(SearchID)]
  data.all.search.interval[, UserID := NULL]

  data.all.search.interval[is.na(UserPrevQryDate), UserPrevQryDate:= 10000000]
  data.all.search.interval[is.na(UserPrevPrevQryDate), UserPrevPrevQryDate:= 10000000]
  data.all.search.interval[is.na(UserPrevPrevPrevQryDate), UserPrevPrevPrevQryDate:= 10000000]
  
  qry.int.breaks <- round(sort(unique(c(-1, 0, 10^(seq(0, 8, 0.5))))))
  data.all.search.interval[, UserQryTotalTimeBin := 
                         cut(UserQryTotalTime, labels=F, breaks=qry.int.breaks)]
  data.all.search.interval[, UserPrevQryDateBin := 
                         cut(UserPrevQryDate, labels=F, breaks=qry.int.breaks)]
  data.all.search.interval[, UserPrevPrevQryDateBin := 
                         cut(UserPrevPrevQryDate, labels=F, 
                             breaks=qry.int.breaks)]
  data.all.search.interval[, UserPrevPrevPrevQryDateBin := 
                         cut(UserPrevPrevPrevQryDate, labels=F, 
                             breaks=qry.int.breaks)]
  
  fn.check.searchid(data.all.search.interval, data.all.search.info.cont, 
                    data.all.search.info.disc)
  
  cols.set <- setdiff(colnames(data.all.search.interval), "SearchID")
  cols.set <- cols.set[!cols.set %like% 'Bin$']
  for (col.nam in cols.set){
    data.all.search.info.cont[
      , col.nam := data.all.search.interval[[col.nam]], with=F]
    col.nam.bin <- paste0(col.nam,"Bin")
    data.all.search.info.disc[
      , col.nam.bin := data.all.search.interval[[col.nam.bin]], with=F]
  }
  
  rm(data.all.search.interval)
  invisible(gc(T))
  
  Store(data.all.search.info.cont, data.all.search.info.disc)
  invisible(gc(T))
  
  data.all.search.type <- data.all.search.info[
    order(UserID, SearchDate),
    list(
      SearchID = SearchID[order(SearchDate)],
      SearchDate = sort(SearchDate),
      SearchOrdUsrDesc = .N:1,
      SearchOrdUsrAsc = 1:.N
    ) ,by="UserID"]
  data.all.search.type[
    , SearchType := factor("hist", levels=c("hist", "tr", "val", "test"))]
  data.all.search.type[
    SearchID %in% unique(data.all.search[ID > 0]$SearchID),
    SearchType := "test"
  ]
  test.min.date <- min(data.all.search.type[SearchType %in% "test", 
                                            list(SearchDate)])
  data.all.search.type[
    SearchType  == "hist" & SearchOrdUsrDesc %in% 1:3
     & SearchDate >= test.min.date,
    SearchType := "val"
  ]
  data.all.search.type[
    SearchType  == "hist" & SearchOrdUsrDesc %in% 4:6
     & SearchDate >= test.min.date,
    SearchType := "tr"
  ]
  data.all.search.type <- data.all.search.type[
    order(SearchID), 
    list(SearchID, SearchOrdUsrAsc, SearchOrdUsrDesc, SearchType)]
  
  fn.check.searchid(data.all.search.info, data.all.search.type)
  
  data.all.search.info[, SearchOrdUsrAsc := data.all.search.type$SearchOrdUsrAsc]
  data.all.search.info[, SearchOrdUsrDesc := data.all.search.type$SearchOrdUsrDesc]
  data.all.search.info[, SearchType := data.all.search.type$SearchType]
  
  setnames(data.all.search.info, 
           c("IPID", "IsUserLoggedOn", "LocationID", "CategoryID"),
           c("UserIPID", "UserLogged", "SearchLocID", "SearchCatID"))
    
  rm(data.all.search.type)
  Store(data.all.search.info)
  invisible(gc(T))
  
  

  data.all.user <- fread(fn.in.file(user.info.file))
  
  cols.search <- colnames(data.all.search.info)
  data.all.search.info <- merge(data.all.search.info,
                                data.all.user,
                                all.x=T,
                                by="UserID")[order(SearchID)]
  fn.check.searchid(data.all.search.info)
  cols.usr <- setdiff(colnames(data.all.user), "UserID")
  for (col.nam in cols.usr) {
      setnames(data.all.search.info, col.nam, 'change_val')
      data.all.search.info[is.na(change_val), change_val := -1]
      setnames(data.all.search.info, 'change_val', col.nam)
  }
  fn.check.searchid(data.all.search.info)
  
  col.ord <- unique(c(cols.search, cols.usr))
  setcolorder(data.all.search.info, col.ord)
  Store(data.all.user, data.all.search.info)
  invisible(gc(T))
  
  
  
  data.all.cat <- fread(fn.in.file(category.file))
  Store(data.all.cat)
  invisible(gc(T))
  
  
  
  data.all.phone <- fread(fn.in.file(phone.request.stream.file))
  data.all.phone[, PhoneRequestDate := as.numeric(fn.parse.date(PhoneRequestDate))]
  data.all.phone <- data.all.phone[order(PhoneRequestDate)]
  
  ids.phone.ad <- sort(unique(data.all.phone$AdID))
  ids.phone.search <- sort(unique(
    data.all.search.full[AdID %in% ids.phone.ad]$SearchID))
  data.phone.search.info <- merge(
    data.all.search.full[SearchID %in% ids.phone.search], 
    data.all.search.info[SearchID %in% ids.phone.search, 
                         list(SearchID, SearchDate, UserIPID, UserID)],
    by="SearchID")
  fn.soar.unload(data.all.search.info, data.all.search.full)
  invisible(gc(T))
  data.phone.search.info <- merge(data.phone.search.info,
                                  data.all.phone,
                                  by=c("UserID", "AdID"))
  
  Store(data.all.phone)
  invisible(gc(T))
  
  data.phone.search.info <- data.phone.search.info[
    PhoneRequestDate <= SearchDate]
  
  data.all.phone.info <- data.phone.search.info[
    ,list(
      UserPrevPhoneRequest = 1
      )
    ,by=c("SearchID")
  ]
  setkeyv(data.all.phone.info, "SearchID")
  
  data.all.phone.info <- data.all.phone.info[J(data.all.search.info$SearchID)]
  data.all.phone.info[, UserPrevPhoneRequest := 
                        as.integer(!is.na(UserPrevPhoneRequest))]

  fn.check.searchid(data.all.phone.info, data.all.search.info)
  data.all.search.info[, UserPrevPhoneRequest := data.all.phone.info$UserPrevPhoneRequest]
  
  rm(data.phone.search.info, ids.phone.ad, ids.phone.search)
  Store(data.all.phone.info, data.all.search.info)
  invisible(gc(T))
  
  data.all.visits <- fread(fn.in.file(visit.stream.file))
  data.all.visits[, ViewDate := as.numeric(fn.parse.date(ViewDate))]
  data.all.visits <- data.all.visits[order(ViewDate)]
  Store(data.all.visits)
  invisible(gc(T))
  
  data.visits.search.info <- merge(
    data.all.search.full, 
    data.all.search.info[, list(SearchID, SearchDate, UserIPID, UserID)],
    by="SearchID")
  
  fn.soar.unload(data.all.search.info, data.all.search.full)
  invisible(gc(T))
  
  data.visits.search.info <- merge(data.visits.search.info,
                                  data.all.visits,
                                  by=c("UserID", "AdID"))
  fn.soar.unload(data.all.visits)
  invisible(gc(T))
  
  data.visits.search.info <- data.visits.search.info[
    SearchDate >= ViewDate | ID >0]
  
  Store(data.visits.search.info)
  
  data.all.visits.info <- data.visits.search.info[
    ,list(
      UserPrevVisitReq = .N,
      UserPrevVisitReqUni = length(unique(AdID))
    )
    ,by=c("SearchID")
    ]
  setkeyv(data.all.visits.info, "SearchID")
  
  data.all.visits.info <- data.all.visits.info[J(data.all.search.info$SearchID)]
  visit.breaks <- sort(unique(c(-1, 0, 1, Inf)))
  data.all.visits.info[is.na(UserPrevVisitReq), UserPrevVisitReq:=0]
  data.all.visits.info[, UserPrevVisitReq := 
                         cut(UserPrevVisitReq, label=F, breaks=visit.breaks)]
  data.all.visits.info[is.na(UserPrevVisitReqUni), UserPrevVisitReqUni:=0]
  data.all.visits.info[, UserPrevVisitReqUni := 
                         cut(UserPrevVisitReqUni, label=F, breaks=visit.breaks)]

  
  fn.check.searchid(data.all.visits.info, data.all.search.info)
  data.all.search.info[, UserPrevVisitReq := data.all.visits.info$UserPrevVisitReq]
  data.all.search.info[, UserPrevVisitReqUni := data.all.visits.info$UserPrevVisitReqUni]
  
  rm(data.visits.search.info, ids.visits.ad, ids.visits.search)
  Store(data.all.visits.info, data.all.search.info)
  invisible(gc(T))

  
  
  data.all.ad <- fread(fn.in.file(ads.info.file))
  data.all.ad <- data.all.ad[
    AdID %in% unique(data.all.search$AdID)]
  
  data.all.ad[is.na(CategoryID), CategoryID := -1]
  data.all.ad[is.na(Price), Price := -1]
  
  ad.price.breaks <- c(c(-Inf, 0), 10^(1:6), 130000000)
  data.all.ad[, PriceBin := cut(Price, labels=F, breaks=ad.price.breaks)]
  
  data.all.ad[, TitleSZ := nchar(Title)]
  ad.title.breaks <- c(-1, 10, 30, 100)
  data.all.ad[, TitleSZBin := cut(TitleSZ, labels=F, breaks=ad.title.breaks)]
  
  data.all.ad[, Params := as.integer(factor(Params))]
  
  data.all.ad[, Title := NULL]
  data.all.ad[, LocationID := NULL]
  data.all.ad[, IsContext := NULL]
  
  setnames(data.all.ad, 
           c("CategoryID", "Price", "PriceBin", "Params", 
             "TitleSZ", "TitleSZBin"),
           c("AdCatID", "AdPrice", "AdPriceBin", "AdParams", 
             "AdTitleSZ", "AdTitleSZBin"))
  
  data.all.ad.info <- merge(data.all.ad,
                            data.all.search[, list(ID, AdID)],
                            by="AdID")[order(ID)]
  
  Store(data.all.ad)
  invisible(gc(T))
  
  fn.check.id(data.all.search, data.all.ad.info)

  data.all.search[, AdCatID:= data.all.ad.info$AdCatID]
  data.all.search[, AdParams:= data.all.ad.info$AdParams]
  
  Store(data.all.search)
  invisible(gc(T))
  
  fn.check.id(data.all.search.disc, data.all.ad.info)
  
  data.all.search.disc[, AdPriceBin:= data.all.ad.info$AdPriceBin]
  data.all.search.disc[, AdTitleSZBin:= data.all.ad.info$AdTitleSZBin]
  
  Store(data.all.search.disc)
  
  fn.check.id(data.all.search.cont, data.all.ad.info)
  
  data.all.search.cont[, AdPrice:= data.all.ad.info$AdPrice]
  data.all.search.cont[, AdTitleSZ:= data.all.ad.info$AdTitleSZ]
  
  Store(data.all.search.cont)
  
  rm(data.all.ad.info)
  invisible(gc(T))
  
  

  data.all.search <- merge(
    data.all.search, 
    data.all.search.info[, list(SearchID, SearchType)],
    by="SearchID")[order(ID)]
  
  col.first <- c("ID", "IsClick", "SearchType", "SearchID")
  col.ord <- c(col.first,
               sort(setdiff(colnames(data.all.search), col.first)))
  setcolorder(data.all.search, col.ord)

  data.all.out.full <- data.all.search[, list(ID, IsClick, Position)]
  setkey(data.all.out.full, ID)
  Store(data.all.out.full)
  invisible(gc(T))

  Store(data.all.search)
  invisible(gc(T))
  
  data.all.search.info.ord <- data.all.search.info[
    , list(SearchID, SearchDate, SearchOrdUsrDesc, SearchType)]
  
  data.all.search.info[, SearchType:= NULL]
  
  data.tr.search.ids <- data.all.search.info.ord[
    SearchType == "hist" & SearchOrdUsrDesc <= 7, SearchID]
  

  Store(data.all.search.info, data.all.search.info.ord,
        data.tr.search.ids)
  
  data.all.search.small <- data.all.search[
    SearchType != "hist"]
  fn.soar.unload(data.all.search)
  
  data.all.out.small <- data.all.search.small[, list(ID, IsClick, Position)]
  Store(data.all.search.small, data.all.out.small)
  invisible(gc(T))

  fn.clean.worker()
}
fn.kill.wk()

toc()

##############################################################
## create linear features data
##############################################################
tic()
cat("Linear features data... \n")

fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
  
  fn.init.worker('data_build/build_lr')
  
  cat("\nCreating dataset\n")
  setkey(data.all.search.info, SearchID)
  setkey(data.all.search.info.disc, SearchID)
  data.all.lr <- merge(data.all.search.info, 
                       data.all.search.info.disc, 
                       by="SearchID")
  fn.soar.unload(data.all.search.info, data.all.search.info.disc)
  
  setkey(data.all.lr, SearchID)
  data.all.lr <- data.all.lr[J(data.all.search$SearchID)]
  
  if (!all(data.all.lr$SearchID == data.all.search$SearchID)) {
    stop('SearchIDs do not match')
  }
  data.all.lr[, ID := data.all.search$ID]
  
  fn.to.data.all.lr <- function(data.add) {
    fn.check.id(data.all.lr, data.add)
    for (col.nam in colnames(data.add)) {
      if (col.nam %ni% colnames(data.all.lr)) {
        print (col.nam)
        data.all.lr[, col.nam := data.add[[col.nam]], with=F]
      }
    }
    invisible(NULL)
  }
  
  fn.to.data.all.lr(data.all.search)
  fn.soar.unload(data.all.search)
  
  fn.to.data.all.lr(data.all.search.disc)
  fn.soar.unload(data.all.search.disc)

  setkeyv(data.all.lr, c("SearchDate", "SearchID", "Position"))

  cat("\nRemoving NAs\n")
  cols.extra <- c("ID", "SearchID", "SearchType", "IsClick", "SearchDate")
  cols.in <- sort(setdiff(colnames(data.all.lr), cols.extra))
  
  for (col.nam in cols.in) {
    if (any(is.na(data.all.lr[[col.nam]]))) {
      setnames(data.all.lr, col.nam, 'change_val')
      data.all.lr[is.na(change_val), change_val := -1]
      setnames(data.all.lr, 'change_val', col.nam)
    }
  }
  
  invisible(gc())
  
  cat("\nSaving dataset data...\n")
  setcolorder(data.all.lr, c(cols.extra, cols.in))
  Store(data.all.lr)
  invisible(gc())
  
  cat("\nSaving dataset csv\n")
  fn.write.csv.chunk(data=data.all.lr,
                     file=fn.out.file("data.all.lr.csv"),
                     compress=F)

  cat("\nSaving small version of dataset\n")  
  cols.in.small <- c(
    "AdID", "AdCatID",  "AdParams",
    "UserID", "UserIPID", "UserAgentID", 
    "UserAgentOSID", "UserDeviceID", "UserAgentFamilyID",
    "SearchLocID", "SearchCatID"
  )
  data.all.lr.small <- data.all.lr[
    , unique(c(cols.extra, "SearchDate", cols.in.small)), with=F]
  fn.soar.unload(data.all.lr)
  Store(data.all.lr.small)
  invisible(gc())

  data.all.lr.id <- data.all.lr.small[
    SearchType != "hist",
    c(cols.extra, "SearchDate"), with=F]
  fn.soar.unload(data.all.lr.small)
  Store(data.all.lr.id)
  invisible(gc())
  
  fn.clean.worker()
}
fn.kill.wk()

toc()


##############################################################
## Probability features
##############################################################
tic()
cat("Probability features data... \n")

fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
  
  fn.init.worker('data_build/prob_features')
  
  cols.in.1way <-c(
    "AdID", "AdCatID",  "AdParams",
    "UserID", "UserIPID", "UserAgentID", 
    "UserAgentOSID", "UserDeviceID", "UserAgentFamilyID",
    "SearchLocID", "SearchCatID"
  )
  
  data.all.prob.1way <- fn.build.prob(cols.in.1way)
  Store(data.all.prob.1way)
  
  cols.in.2way <-c(
    "AdID", "AdCatID",
    "UserID", "UserIPID", "UserAgentOSID", "UserAgentFamilyID",
    "SearchLocID", "SearchCatID"
  )
  cols.in.2way <- cols.in.1way
  
  data.all.prob.2way.ad.us <- fn.build.prob(
    fn.build.interaction(cols.in.2way, c("Ad", "Us")))
  Store(data.all.prob.2way.ad.us)
  
  data.all.prob.2way.ad.srch <- fn.build.prob(
    fn.build.interaction(cols.in.2way, c("Ad", "Search")))
  Store(data.all.prob.2way.ad.srch)
  
  data.all.prob.2way.us.srch <- fn.build.prob(
    fn.build.interaction(cols.in.2way, c("Us", "Search")))
  Store(data.all.prob.2way.us.srch)
  
  data.all.prob.2way.srch <- fn.build.prob(
    fn.build.interaction(cols.in.2way, c("Search", "Search")))
  Store(data.all.prob.2way.srch)
  
  fn.clean.worker()
}
fn.kill.wk()

toc()
