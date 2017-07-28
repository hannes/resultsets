res <- readRDS(res, "all.rds")
nums <- res[, grep("num_", names(res))]
grid <- expand.grid(names(nums), names(nums)) 
grid$c1 <- 0L
grid$c2 <- 0L

for (i in 1:nrow(grid)) {
	print(grid[i, ])
	p1 <- nums[nums[, grid$Var1[i]] > 0, grid$Var2[i], drop=FALSE]
	grid$c1[i] <- nrow(p1)
	grid$c2[i] <- nrow(p1[p1[, 1] > 0, , drop=FALSE])
}

insurance <- grid

saveRDS(grid, "grid.rds")

grid$perc <- round(grid$c2/grid$c1, 2)
grid <- grid[grid$Var1 != grid$Var2 && grid$perc < 1 && grid$perc > 0.5 && grid$c1 > 100, ]
