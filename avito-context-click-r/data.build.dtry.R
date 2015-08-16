source("_utils.R")

### read train and test datasets ###
data.train <- fread(fn.in.file(train.file))
data.train[, ID := -(SearchID*10 + Position)]
data.test <- fread(fn.in.file(test.file))
data.all <- rbindlist(list(data.train, data.test), fill = T)
rm(data.train, data.test)
gc()

### add Lucas cv splitting ###
data.all.split <- data.all.search[,list(SearchType, SearchID)]
fn.soar.unload(data.all.search)
invisible(gc(T))
setkey(data.all.split, SearchType, SearchID)
data.all.split <- unique(data.all.split)
setkey(data.all, SearchID)
setkey(data.all.split, SearchID)
data.all <- merge(data.all, data.all.split, by="SearchID", all.x=T)
data.all[, SearchType := as.character(SearchType)]
data.all[is.na(SearchType), SearchType := "0"]
data.all[SearchType=="hist", SearchType := "0"]
data.all[SearchType=="test", SearchType := "3"]
data.all[SearchType=="val", SearchType := "2"]
data.all[SearchType=="tr", SearchType := "1"]
data.all[, SearchType := as.numeric(SearchType)]
data.all[is.na(IsClick), IsClick := 0]
rm(data.all.split)
gc()

data.search.feat <- data.all[,list(SearchObjectType3Count = sum(ObjectType==3),
                                   SearchObjectType2Count = sum(ObjectType==2),
                                   SearchObjectType1Count = sum(ObjectType==1),
                                   #SearchObjectType12Count = sum(ObjectType!=3),
                                   #SearchAdCount = .N,
                                   SearchPosition1Count = sum(Position==1),
                                   SearchPosition2Count = sum(Position==2),
                                   SearchPosition6Count = sum(Position==6),
                                   SearchPosition7Count = sum(Position==7),
                                   SearchPosition8Count = sum(Position==8),
                                   SearchAdUniqueCount = length(unique(AdID))),by="SearchID"]
data.ad.feat <- data.all[, list(#AdObjectTypeUniqueCount = length(unique(ObjectType)),
                                AdPosition1Count = sum(Position==1),
                                #AdPosition2Count = sum(Position==2),
                                #AdPosition6Count = sum(Position==6),
                                AdPosition7Count = sum(Position==7)), by="AdID"]
                                #AdPosition8Count = sum(Position==8)), by="AdID"]

#AdPosition12Count = sum(Position==1 | Position==2),
#AdPosition678Count = sum(Position>2)
data.all <- data.all[ObjectType==3]
gc()
data.all[, ObjectType := NULL]
setkey(data.all, SearchID)
setkey(data.search.feat, SearchID)
data.all <- merge(data.all, data.search.feat, by="SearchID", all.x=T)
rm(data.search.feat)
gc()

setkey(data.all, AdID)
setkey(data.ad.feat, AdID)
data.all <- merge(data.all, data.ad.feat, by="AdID", all.x=T)
rm(data.ad.feat)
gc()

### save dataset ###
setkey(data.all, ID)

### first set of features ###
flist.basic <- c("ID", "SearchID", "AdID", "Position", "HistCTR", "SearchType", "IsClick")
flist1 <- setdiff(colnames(data.all), flist.basic)
data.feats1 <- data.all[, flist1, with=F]
save(data.feats1, file=fn.rdata.file("data.feats1.RData"))
rm(data.feats1)
gc()

### basic set of features ###
data.id <- data.all[,flist.basic,with=F]
rm(data.all)
gc()
save(data.id, file=fn.rdata.file("data.id.RData"))

### generate search features ###
data.search.info <- fread(fn.in.file(search.info.file))
setnames(data.search.info, "LocationID", "SearchLocationID")
setnames(data.search.info, "CategoryID", "SearchCategoryID")
data.search.info[, SearchDayYear := as.numeric(format(as.Date(SearchDate, format="%Y-%m-%d"), format = "%j"))]
#data.search.info[, SearchWeekYear := as.numeric(format(as.Date(SearchDate, format="%Y-%m-%d"), format = "%V"))]
#data.search.info[, SearchWeekday := as.numeric(format(as.Date(SearchDate, format="%Y-%m-%d"), format = "%w"))]
#data.search.info[SearchWeekday==0, SearchWeekday := 7]
data.search.info[, SearchDate := NULL]
data.search.info[, SearchQuerySize := nchar(SearchQuery)]
russian.alphabet <- c("а|б|в|г|д|е|ж|з|и|й|к|л|м|н|о|п|р|с|т|у|ф|х|ц|ч|ш|щ|ъ|ы|ь|э|ю|я")
data.search.info[, SearchRussian := as.numeric(grepl(russian.alphabet, SearchQuery))]
data.search.info[, SearchQuery := NULL]
data.search.info[, SearchParamsSize := nchar(SearchParams)]
data.search.info[, SearchParams2 := gsub(":","",SearchParams)]
data.search.info[, SearchParamsCount := SearchParamsSize - nchar(SearchParams2)]
data.search.info[, SearchParams := NULL]
data.search.info[, SearchParams2 := NULL]
gc()

data.search.feats <- data.search.info[,list(UserSearchUniqueCount = length(unique(SearchID)),
                                            UserSearchLocationUniqueCount = length(unique(SearchLocationID)),
                                            UserSearchCategoryUniqueCount = length(unique(SearchCategoryID))),by="UserID"]
                                            #UserIPUniqueCount = length(unique(IPID))),by="UserID"]
setkey(data.search.feats, UserID)
setkey(data.search.info, UserID)
data.feats2 <- merge(data.search.info, data.search.feats, by="UserID", all.x=T)
gc()

data.search.feats <- data.search.info[,list(LocationUserUniqueCount = length(unique(UserID))),by="SearchLocationID"]
setkey(data.search.feats, SearchLocationID)
setkey(data.feats2, SearchLocationID)
data.feats2 <- merge(data.feats2, data.search.feats, by="SearchLocationID", all.x=T)

data.search.feats <- data.search.info[,list(CategoryUserUniqueCount = length(unique(UserID))),by="SearchCategoryID"]
setkey(data.search.feats, SearchCategoryID)
setkey(data.feats2, SearchCategoryID)
data.feats2 <- merge(data.feats2, data.search.feats, by="SearchCategoryID", all.x=T)
rm(data.search.feats)
gc()

data.search.feats <- data.search.info[,list(UserID, SearchDayYear)]
data.search.feats <- data.search.feats[,list(Count = .N), by=c("UserID", "SearchDayYear")]
for (days.before in c(1:7)) {
  data.search.feats2 <- data.search.feats[,list(UserID, SearchDayYear, Count)]
  data.search.feats2[, SearchDayYear := SearchDayYear+days.before]
  setnames(data.search.feats2, "Count", "CountPrevious")
  setkey(data.search.feats, UserID, SearchDayYear)
  setkey(data.search.feats2, UserID, SearchDayYear)
  data.search.feats <- merge(data.search.feats, data.search.feats2, by=c("UserID", "SearchDayYear"), all.x=T)
  data.search.feats[is.na(CountPrevious), CountPrevious := 0]
  setnames(data.search.feats, "CountPrevious", paste0("CountPrevious",days.before))
}
rm(data.search.feats2)
data.search.feats[, UserSearchCount3 := Count + CountPrevious1 + CountPrevious2]
data.search.feats[, UserSearchCount7 := Count + CountPrevious1 + CountPrevious2 + CountPrevious3 + CountPrevious4 + CountPrevious5 + CountPrevious6 + CountPrevious7]
data.search.feats <- data.search.feats[,list(UserID,
                                             SearchDayYear,
                                             UserSearchCount3,
                                             UserSearchCount7)]
setkey(data.search.feats, UserID, SearchDayYear)
setkey(data.feats2, UserID, SearchDayYear)
data.feats2 <- merge(data.feats2, data.search.feats, by=c("UserID","SearchDayYear"), all.x=T)
rm(data.search.feats)
gc()

setkey(data.search.info, UserID, SearchDayYear, SearchID)
data.search.feats <- data.search.info[,list(SearchID = SearchID[2:(.N)],
                                            SearchIDPreviousAge = SearchDayYear[2:(.N)] - SearchDayYear[1:(.N-1)]),by="UserID"]
data.search.feats <- data.search.feats[!is.na(SearchIDPreviousAge)]
setkey(data.search.feats, UserID, SearchID)
setkey(data.feats2, UserID, SearchID)
data.feats2 <- merge(data.feats2, data.search.feats, by=c("UserID","SearchID"), all.x=T)
rm(data.search.feats)
gc()
data.feats2[is.na(SearchIDPreviousAge), SearchIDPreviousAge := -1]
rm(data.search.info)
gc()
load(fn.rdata.file("data.id.RData"))
setkey(data.id, SearchID)
setkey(data.feats2, SearchID)
data.feats2 <- merge(data.id, data.feats2, by=c("SearchID"), all.x=T)
setkey(data.feats2, ID)
flist.basic <- c("ID", "SearchID", "SearchLocationID", "SearchCategoryID", "SearchDayYear", "AdID", "UserID", "Position", "HistCTR", "SearchType", "IsClick")
flist2 <- setdiff(colnames(data.feats2), flist.basic)
data.id <- data.feats2[,flist.basic,with=F]
save(data.id, file=fn.rdata.file("data.id.RData"))
rm(data.id)
gc()
data.feats2 <- data.feats2[, flist2, with=F]
save(data.feats2, file=fn.rdata.file("data.feats2.RData"))
rm(data.feats2)
gc()

data.ads.info <- fread(fn.in.file(ads.info.file))
data.ads.info[, LocationID := NULL]
setnames(data.ads.info, "CategoryID", "AdCategoryID")
setnames(data.ads.info, "Params", "AdParams")
setnames(data.ads.info, "Title", "AdTitle")
data.ads.info[, AdParamsSize := nchar(AdParams)]
data.ads.info[, AdParams2 := gsub(":","",AdParams)]
data.ads.info[, AdParamsCount := AdParamsSize - nchar(AdParams2)]
data.ads.info[,AdParams := NULL]
data.ads.info[,AdParams2 := NULL]
data.ads.info[, AdTitleSize := nchar(AdTitle)]
data.ads.info[, AdTitle := NULL]
data.ads.info[, IsContext := NULL]
load(fn.rdata.file("data.id.RData"))
setkey(data.id, AdID)
setkey(data.ads.info, AdID)
data.feats3 <- merge(data.id, data.ads.info, by=c("AdID"), all.x=T)

data.feats <- data.feats3[,list(AdSearchUniqueCount = length(unique(SearchID))),by="AdID"]
setkey(data.feats3, AdID)
setkey(data.feats, AdID)
data.feats3 <- merge(data.feats3, data.feats, by="AdID", all.x=T)

data.feats <- data.feats3[,list(UserAdUniqueCount = length(unique(AdID))),by="UserID"]
setkey(data.feats3, UserID)
setkey(data.feats, UserID)
data.feats3 <- merge(data.feats3, data.feats, by="UserID", all.x=T)

data.feats <- data.feats3[,list(UserAdCount = .N), by=c("UserID", "AdID")]
setkey(data.feats3, UserID, AdID)
setkey(data.feats, UserID, AdID)
data.feats3 <- merge(data.feats3, data.feats, by=c("UserID","AdID"), all.x=T)

data.feats3[is.na(AdCategoryID), AdCategoryID := -1]
data.feats <- data.feats3[,list(AdCategoryPriceMedian = median(Price)), by=c("AdCategoryID")]
setkey(data.feats3, AdCategoryID)
setkey(data.feats, AdCategoryID)
data.feats3 <- merge(data.feats3, data.feats, by=c("AdCategoryID"), all.x=T)
data.feats3[, AdCategoryPriceDeviation := round((Price - AdCategoryPriceMedian)/AdCategoryPriceMedian, 6)]
data.feats3[is.na(Price), Price := 0]
data.feats3[is.na(AdCategoryPriceDeviation), AdCategoryPriceDeviation := 0]
data.feats3[, AdCategoryPriceMedian := NULL]
rm(data.feats, data.ads.info)
gc()

setkey(data.feats3, ID)
flist.basic <- c("ID", "SearchID", "SearchLocationID", "SearchCategoryID", "SearchDayYear", "AdID", "AdCategoryID", "UserID", "Position", "HistCTR", "SearchType", "IsClick")
flist3 <- setdiff(colnames(data.feats3), flist.basic)
data.id <- data.feats3[,flist.basic,with=F]
save(data.id, file=fn.rdata.file("data.id.RData"))
rm(data.id)
data.feats3 <- data.feats3[, flist3, with=F]
save(data.feats3, file=fn.rdata.file("data.feats3.RData"))
rm(data.feats3)
gc()

### user info file ###
data.user.info <- fread(fn.in.file(user.info.file))
load(fn.rdata.file("data.id.RData"))
setkey(data.id, UserID)
setkey(data.user.info, UserID)
data.feats4 <- merge(data.id, data.user.info, by=c("UserID"), all.x=T)
setkey(data.feats4, ID)
flist.basic <- c("ID", "SearchID", "SearchLocationID", "SearchCategoryID", "SearchDayYear", "AdID", "AdCategoryID", "UserID", "Position", "HistCTR", "SearchType", "IsClick")
flist4 <- setdiff(colnames(data.feats4), flist.basic)
data.feats4 <- data.feats4[, flist4, with=F]
save(data.feats4, file=fn.rdata.file("data.feats4.RData"))
rm(data.feats4, data.user.info)
gc()

### category and location features ###
data.category <- fread(fn.in.file(category.file))
setnames(data.category, "CategoryID", "SearchCategoryID")
setnames(data.category, "Level", "CategoryLevel")
data.location <- fread(fn.in.file(location.file))
setnames(data.location, "LocationID", "SearchLocationID")
setnames(data.location, "Level", "LocationLevel")
load(fn.rdata.file("data.id.RData"))
setkey(data.id, SearchCategoryID)
setkey(data.category, SearchCategoryID)
data.feats5 <- merge(data.id, data.category, by=c("SearchCategoryID"), all.x=T)
setkey(data.feats5, SearchLocationID)
setkey(data.location, SearchLocationID)
data.feats5 <- merge(data.feats5, data.location, by=c("SearchLocationID"), all.x=T)
setkey(data.feats5, ID)
flist.basic <- c("ID", "SearchID", "SearchLocationID", "SearchCategoryID", "SearchDayYear", "AdID", "AdCategoryID", "UserID", "Position", "HistCTR", "SearchType", "IsClick")
flist5 <- setdiff(colnames(data.feats5), flist.basic)
data.feats5 <- data.feats5[, flist5, with=F]
save(data.feats5, file=fn.rdata.file("data.feats5.RData"))
rm(data.feats5, data.category, data.location)
gc()

### visits stream ###
data.visits.stream <- fread(fn.in.file(visits.stream.file))
data.visits.stream[, ViewDayYear := as.numeric(format(as.Date(ViewDate, format="%Y-%m-%d"), format = "%j"))]
data.visits.stream[, ViewDate := NULL]
gc()
data.ads.info <- fread(fn.in.file(ads.info.file))
setnames(data.ads.info, "CategoryID", "AdCategoryID")
data.ads.info <- data.ads.info[,list(AdID, AdCategoryID, Price)]
gc()
setkey(data.ads.info, AdID)
setkey(data.visits.stream, AdID)
data.visits.stream <- merge(data.visits.stream, data.ads.info, by="AdID", all.x=T)
load(fn.rdata.file("data.id.RData"))

data.feats <- data.visits.stream[,list(UserAdViewTotalCount = .N,
                                       UserAdViewUniqueCount = length(unique(AdID))),by="UserID"]
setkey(data.id, UserID)
setkey(data.feats, UserID)
data.feats6 <- merge(data.id, data.feats, "UserID", all.x=T)
rm(data.id, data.feats)
gc()

data.feats <- data.visits.stream[,list(UserAdCategoryPriceMean = round(mean(Price), 6),
                                       UserAdCategoryPriceMedian = median(Price),
                                       UserAdCategoryPriceMin = min(Price),
                                       UserAdCategoryPriceMax = max(Price)),by=c("UserID","AdCategoryID")]
setkey(data.feats6, UserID, AdCategoryID)
setkey(data.feats, UserID, AdCategoryID)
data.feats6 <- merge(data.feats6, data.feats, c("UserID", "AdCategoryID"), all.x=T)

rm(data.feats, data.visits.stream, data.ads.info)
gc()

setkey(data.feats6, ID)
flist.basic <- c("ID", "SearchID", "SearchLocationID", "SearchCategoryID", "SearchDayYear", "AdID", "AdCategoryID", "UserID", "Position", "HistCTR", "SearchType", "IsClick")
flist6 <- setdiff(colnames(data.feats6), flist.basic)
data.feats6 <- data.feats6[, flist6, with=F]
for (col in colnames(data.feats6)) {
  setnames(data.feats6, col, "feature")
  data.feats6[is.na(feature), feature := -1]
  setnames(data.feats6, "feature", col)
}
save(data.feats6, file=fn.rdata.file("data.feats6.RData"))
rm(data.feats6)
gc()

### phone requests stream ###
data.phone.requests.stream <- fread(fn.in.file(phone.requests.stream.file))
data.phone.requests.stream[, PhoneRequestDayYear := as.numeric(format(as.Date(PhoneRequestDate, format="%Y-%m-%d"), format = "%j"))]
data.phone.requests.stream[, PhoneRequestDate := NULL]
gc()
data.ads.info <- fread(fn.in.file(ads.info.file))
setnames(data.ads.info, "CategoryID", "AdCategoryID")
data.ads.info <- data.ads.info[,list(AdID, AdCategoryID, Price)]
gc()
setkey(data.ads.info, AdID)
setkey(data.phone.requests.stream, AdID)
data.phone.requests.stream <- merge(data.phone.requests.stream, data.ads.info, by="AdID", all.x=T)
load(fn.rdata.file("data.id.RData"))

data.feats <- data.phone.requests.stream[,list(UserAdViewTotalCount2 = .N,
                                               UserAdViewUniqueCount2 = length(unique(AdID))),by="UserID"]
setkey(data.id, UserID)
setkey(data.feats, UserID)
data.feats7 <- merge(data.id, data.feats, "UserID", all.x=T)
rm(data.id, data.feats)
gc()

data.feats <- data.phone.requests.stream[,list(UserAdCategoryPriceMean2 = round(mean(Price), 6),
                                               UserAdCategoryPriceMedian2 = median(Price),
                                               UserAdCategoryPriceMin2 = min(Price),
                                               UserAdCategoryPriceMax2 = max(Price)),by=c("UserID","AdCategoryID")]
setkey(data.feats7, UserID, AdCategoryID)
setkey(data.feats, UserID, AdCategoryID)
data.feats7 <- merge(data.feats7, data.feats, c("UserID", "AdCategoryID"), all.x=T)

rm(data.feats, data.phone.requests.stream, data.ads.info)
gc()

setkey(data.feats7, ID)
flist.basic <- c("ID", "SearchID", "SearchLocationID", "SearchCategoryID", "SearchDayYear", "AdID", "AdCategoryID", "UserID", "Position", "HistCTR", "SearchType", "IsClick")
flist7 <- setdiff(colnames(data.feats7), flist.basic)
data.feats7 <- data.feats7[, flist7, with=F]
for (col in colnames(data.feats7)) {
  setnames(data.feats7, col, "feature")
  data.feats7[is.na(feature), feature := -1]
  setnames(data.feats7, "feature", col)
}
save(data.feats7, file=fn.rdata.file("data.feats7.RData"))
rm(data.feats7)
gc()

#############################################################################
################ MAKE DATASET AND LIKELIHOOD FEATURES #######################
#############################################################################

spar <- 30

load(fn.rdata.file("data.id.RData"))
is.click <- data.id[,IsClick]
ids <- data.id[,ID]
search.type <- data.id[,SearchType]
global.avg <- round(mean(data.id[SearchType<3][,IsClick]), 5)
data.reduced <- data.id[SearchType>0][,list(ID,
                                            IsClick,
                                            SearchType,
                                            SearchDayYear,
                                            Position,
                                            HistCTR,
                                            AdID,
                                            UserID,
                                            SearchLocationID,
                                            SearchCategoryID,
                                            AdCategoryID)]

feats <- c("AdID",
           "UserID",
           "SearchLocationID",
           "SearchCategoryID",
           "AdCategoryID")
for (feat in feats) {
  cat("Processing feature", feat, "...\n")
  data.likeli.list <- list()
  for (st in c(0:2)) {
    data.likeli <- data.id[SearchType<=st][,list(likeli = round((sum(IsClick) + spar*global.avg)/(.N+spar), 5),
                                                 SearchType = st+1),by=feat]
    data.likeli.list[[length(data.likeli.list)+1]] <- data.likeli
  }
  rm(data.likeli)
  invisible(gc())
  data.likeli.list <- rbindlist(data.likeli.list, use.names=T, fill=F)
  setkeyv(data.likeli.list, c("SearchType",feat))
  setkeyv(data.reduced, c("SearchType",feat))
  data.reduced <- merge(data.reduced, data.likeli.list, by=c("SearchType", feat), all.x=T)
  data.reduced[is.na(likeli), likeli := global.avg]
  setnames(data.reduced, "likeli", paste0(feat,"likeli"))
  data.reduced[, feat := NULL, with=F]
  rm(data.likeli.list)
  invisible(gc())
}
rm(data.id)
gc()
data.reduced.all <- copy(data.reduced)
setkey(data.reduced.all, ID)

##### data.feats1 ######
load(fn.rdata.file("data.feats1.RData"))
data.feats1[, ID := ids]
data.feats1[, SearchType := search.type]
data.reduced <- data.feats1[SearchType>0]
data.reduced[,SearchType := NULL]
setkey(data.reduced, ID)
flist <- setdiff(colnames(data.reduced),
                 c("SearchObjectType2Count",
                   "SearchPosition1Count",
                   "SearchPosition8Count",
                   "SearchAdUniqueCount"))
data.reduced.all <- merge(data.reduced.all, data.reduced[,flist,with=F], by="ID", all.x=T)
rm(data.feats1)
gc()

##### data.feats2 ######
load(fn.rdata.file("data.feats2.RData"))
data.feats2[, ID := ids]
data.feats2[, SearchType := search.type]
data.feats2[, IsClick := is.click]
data.reduced <- data.feats2[SearchType>0]
data.reduced[,SearchType := NULL]
data.reduced[,IsClick := NULL]
setkey(data.reduced, ID)
flist <- setdiff(colnames(data.reduced),
                 c("UserSearchLocationUniqueCount",
                   "UserSearchCategoryUniqueCount",
                   "UserSearchCount7",
                   "UserSearchCount3",
                   "IPID"))
data.reduced.all <- merge(data.reduced.all, data.reduced[,flist,with=F], by="ID", all.x=T)

##### data.feats3 ######
load(fn.rdata.file("data.feats3.RData"))
data.feats3[, ID := ids]
data.feats3[, SearchType := search.type]
data.reduced <- data.feats3[SearchType>0]
data.reduced[,SearchType := NULL]
setkey(data.reduced, ID)
flist <- setdiff(colnames(data.reduced),
                 c("AdSearchUniqueCount",
                   "UserAdUniqueCount"))
data.reduced.all <- merge(data.reduced.all, data.reduced[,flist,with=F], by="ID", all.x=T)
rm(data.feats3)
gc()

##### data.feats4 ######
load(fn.rdata.file("data.feats4.RData"))
data.feats4[, ID := ids]
data.feats4[, SearchType := search.type]
data.feats4[, IsClick := is.click]
data.reduced <- data.feats4[SearchType>0]
feats <- c("UserAgentID",
           "UserAgentOSID",
           "UserDeviceID",
           "UserAgentFamilyID")
for (feat in feats) {
  cat("Processing feature", feat, "...\n")
  data.likeli.list <- list()
  for (st in c(0:2)) {
    data.likeli <- data.feats4[SearchType<=st][,list(likeli = round((sum(IsClick) + spar*global.avg)/(.N+spar), 5),
                                                     SearchType = st+1),by=feat]
    data.likeli.list[[length(data.likeli.list)+1]] <- data.likeli
  }
  rm(data.likeli)
  invisible(gc())
  data.likeli.list <- rbindlist(data.likeli.list, use.names=T, fill=F)
  setkeyv(data.likeli.list, c("SearchType",feat))
  setkeyv(data.reduced, c("SearchType",feat))
  data.reduced <- merge(data.reduced, data.likeli.list, by=c("SearchType", feat), all.x=T)
  data.reduced[is.na(likeli), likeli := global.avg]
  setnames(data.reduced, "likeli", paste0(feat,"likeli"))
  data.reduced[, feat := NULL, with=F]
  rm(data.likeli.list)
  invisible(gc())
}
rm(data.feats4)
gc()
data.reduced[,SearchType := NULL]
data.reduced[,IsClick := NULL]
setkey(data.reduced, ID)
data.reduced.all <- merge(data.reduced.all, data.reduced, by="ID", all.x=T)

##### data.feats6 ######
load(fn.rdata.file("data.feats6.RData"))
data.feats6[, ID := ids]
data.feats6[, SearchType := search.type]
data.reduced <- data.feats6[SearchType>0]
data.reduced[,SearchType := NULL]
setkey(data.reduced, ID)
data.reduced.all <- merge(data.reduced.all, data.reduced, by="ID", all.x=T)
rm(data.feats6)
gc()

##### data.feats7 ######
load(fn.rdata.file("data.feats7.RData"))
data.feats7[, ID := ids]
data.feats7[, SearchType := search.type]
data.reduced <- data.feats7[SearchType>0]
data.reduced[,SearchType := NULL]
setkey(data.reduced, ID)
data.reduced.all <- merge(data.reduced.all, data.reduced, by="ID", all.x=T)
rm(data.feats7)
gc()

rm(ids, is.click, search.type)
gc()

#### add 2-way likelihood #####
load(fn.rdata.file("data.id.RData"))
data.feats <- data.id[,list(ID,
                            SearchType,
                            IsClick,
                            UserID,
                            AdID,
                            SearchLocationID,
                            SearchCategoryID,
                            AdCategoryID)]
rm(data.id)
invisible(gc())
load(fn.rdata.file("data.feats2.RData"))
data.feats[, IPID := data.feats2$IPID]
rm(data.feats2)
invisible(gc())
load(fn.rdata.file("data.feats4.RData"))
data.feats[, UserAgentOSID := data.feats4$UserAgentOSID]
data.feats[, UserAgentFamilyID := data.feats4$UserAgentFamilyID]
rm(data.feats4)
invisible(gc())
data.reduced <- data.feats[SearchType>0]
flist <- setdiff(colnames(data.feats), c("ID", "SearchType", "IsClick"))
for (i in 1:(length(flist)-1)) {
  for (j in (i+1):length(flist)) {
    f1 <- flist[i]
    f2 <- flist[j]
    cat("Processing features", f1, f2, "...\n")
    data.group <- data.feats[,list(group = .GRP), by=c(f1, f2)]
    setkeyv(data.group, c(f1,f2))
    setkeyv(data.feats, c(f1,f2))
    setkeyv(data.reduced, c(f1,f2))
    data.feats <- merge(data.feats, data.group, by=c(f1, f2), all.x=T)
    data.reduced <- merge(data.reduced, data.group, by=c(f1, f2), all.x=T)
    data.likeli.list <- list()
    for (st in c(0:2)) {
      data.likeli <- data.feats[SearchType<=st][,list(likeli = round((sum(IsClick) + spar*global.avg)/(.N+spar), 5),
                                                      SearchType = st+1),by="group"]
      data.likeli.list[[length(data.likeli.list)+1]] <- data.likeli
    }
    rm(data.likeli)
    invisible(gc())
    data.likeli.list <- rbindlist(data.likeli.list, use.names=T, fill=F)
    setkeyv(data.likeli.list, c("SearchType","group"))
    setkeyv(data.reduced, c("SearchType","group"))
    data.reduced <- merge(data.reduced, data.likeli.list, by=c("SearchType", "group"), all.x=T)
    data.reduced[is.na(likeli), likeli := global.avg]
    setnames(data.reduced, "likeli", paste0(f1,f2,"likeli"))
    data.reduced[, group := NULL]
    data.feats[, group := NULL]
    rm(data.likeli.list)
    invisible(gc())
  }
}
rm(data.feats, data.group)
for (f in flist) {
  data.reduced[, f := NULL, with=F]
}
data.reduced[,SearchType := NULL]
data.reduced[,IsClick := NULL]
setkey(data.reduced, ID)
data.reduced.all <- merge(data.reduced.all, data.reduced, by="ID", all.x=T)
gc()

rm(data.reduced)
gc()
save(data.reduced.all, file=fn.rdata.file("data.reduced.all.RData"))
rm(data.reduced.all)
gc()

#############################################################################
########################### ADD TEXT FEATURES ###############################
#############################################################################

data.ads.info <- fread(fn.in.file(ads.info.file))
setnames(data.ads.info, "Title", "AdTitle")
data.ads.info <- data.ads.info[,list(AdID, AdTitle)]
data.ads.info[, AdTitleSize := nchar(AdTitle)]
data.ads.info <- data.ads.info[AdTitleSize>0]
data.ads.info[, AdTitleSize := NULL]
gc()

data.ads.info[, AdTitle := removePunctuation(AdTitle)]
data.ads.info[, AdTitle := stripWhitespace(AdTitle)]
data.ads.info[, AdTitle := tolower(AdTitle)]

data.ads.info <- data.ads.info[, list(AdTitleWord = unlist(strsplit(AdTitle, " "))), by="AdID"]
data.ads.info[, AdTitleWordSize := nchar(AdTitleWord)]
data.ads.info <- data.ads.info[AdTitleWordSize>2]
data.ads.info[, AdTitleWordSize := NULL]
gc()

data.search.info <- fread(fn.in.file(search.info.file))
data.search.info <- data.search.info[,list(SearchID, SearchQuery)]
gc()
data.search.info[, SearchQuerySize := nchar(SearchQuery)]
data.search.info <- data.search.info[SearchQuerySize>0]
data.search.info[, SearchQuerySize := NULL]

data.search.info[, SearchQuery := removePunctuation(SearchQuery)]
data.search.info[, SearchQuery := stripWhitespace(SearchQuery)]
data.search.info[, SearchQuery := tolower(SearchQuery)]
data.search.info <- data.search.info[, list(SearchQueryWord = unlist(strsplit(SearchQuery, " "))), by="SearchID"]
data.search.info[, SearchQueryWordSize := nchar(SearchQueryWord)]
data.search.info <- data.search.info[SearchQueryWordSize>2]
data.search.info[, SearchQueryWordSize := NULL]
gc()

load(fn.rdata.file("data.id.RData"))
data.click <- data.id[SearchType>0][,list(SearchID,AdID)]
rm(data.id)
gc()
data.click <- merge(data.click, data.ads.info, all.x=T, allow.cartesian=T, by="AdID")
data.click <- merge(data.click, data.search.info, all.x=T, allow.cartesian=T, by="SearchID")
data.click <- data.click[!is.na(SearchQueryWord)]
data.click <- data.click[,list(SearchAdCommonWordCount = sum(AdTitleWord==SearchQueryWord)),by=c("SearchID", "AdID")]

load(fn.rdata.file("data.id.RData"))
setkey(data.id, ID)
load(fn.rdata.file("data.reduced.all.RData"))
setkey(data.reduced.all, ID)
data.reduced.all <- merge(data.reduced.all, data.id[,list(ID, 
                                                          AdID,
                                                          SearchID)], by="ID", all.x=T)
rm(data.id)
gc()
setkey(data.reduced.all, AdID, SearchID)
setkey(data.click, AdID, SearchID)
data.reduced.all <- merge(data.reduced.all, data.click, by=c("AdID", "SearchID"), all.x=T)
data.reduced.all[is.na(SearchAdCommonWordCount), SearchAdCommonWordCount := 0]
data.reduced.all[,AdID := NULL]
data.reduced.all[,SearchID := NULL]
setkey(data.reduced.all, ID)
save(data.reduced.all, file=fn.rdata.file("data.reduced.all.RData"))
gc()
