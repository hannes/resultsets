aa <- data.table::fread("queries_analyzed/all.res", sep="\t", quote="", header=F)
aa$id <- 1:nrow(aa)

splitty <- compiler::cmpfun(function(id) {
	s1 <- strsplit(aa$V6[[id]], ",", fixed=T)[[1]]
	s2 <- strsplit(s1, "=", fixed=T)
	nm <- vapply(s2, function(z) {z[[1]]}, FUN.VALUE=character(1))
	vl <- vapply(s2, function(z) {as.integer(z[[2]])}, FUN.VALUE=integer(1))
	list(id=rep(id, length(vl)), key=nm, value=vl)
})

b <- data.table::rbindlist(parallel::mclapply(aa$id, splitty, mc.cores=4))

kv <- tidyr::spread(b, "key", "value", fill = 0)
aa$V6 <- NULL

library(dplyr)
res <- aa %>% inner_join(kv, by="id")

saveRDS(res, "all.rds")
