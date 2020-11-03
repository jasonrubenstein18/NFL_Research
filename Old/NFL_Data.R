# Must install the devtools package using the below commented out code
install.packages('devtools')
install.packages('ggjoy')
library(devtools)
library(tidyverse)
library(ggplot2)
library(ggjoy)

devtools::install_github(repo = "maksimhorowitz/nflscrapR")
library(nflscrapR)

pbp_2014 <- season_play_by_play(2014)
pbp_2015 <- season_play_by_play(2015)
pbp_2016 <- season_play_by_play(2016)
pbp_2017 <- season_play_by_play(2017)
pbp_2018 <- season_play_by_play(2018)
pbp_2019 <- season_play_by_play(2019)

pbp_data = bind_rows(pbp_2014, pbp_2015, pbp_2016, pbp_2017, pbp_2018, pbp_2019)

# update path for your own machine
write.csv(pbp_data,"~/Desktop/R/NFL/NFL_pbp_data.csv", row.names = FALSE)


# # remove QB rushes which tend to have more extreme impact on EPA
# non_qb_rush = pbp_data[!grepl('scramble', pbp_data$desc),]

# # filter data and find EPA over total rush attempts by season
# rushing_stats <- non_qb_rush %>% filter(PassAttempt == 0 & PlayType != "No Play" & 
#                                     !is.na(Rusher)) %>% group_by(Season, Rusher) %>% summarise(RushAttempt = n(), 
#                                      Total_EPA = sum(EPA, na.rm = TRUE), EPA_per_Att = Total_EPA/RushAttempt) %>% 
#                                      filter(RushAttempt >= 50)

# # Plot EPA per rush attempt by season
# ggplot(rushing_stats, aes(x = EPA_per_Att, y = as.factor(Season))) + geom_joy(scale = 3, 
#                       rel_min_height = 0.01) + theme_light() + ylab("Season") + xlab("EPA per Rush Attempt") + 
#                       scale_y_discrete(expand = c(0.01, 0)) + scale_x_continuous(expand = c(0.01,
#                       0)) + ggtitle("The Shifting Distribution of EPA per Rush Attempt") + theme(plot.title = element_text(hjust = 0.5, 
#                       size = 16), axis.title = element_text(size = 16), axis.text = element_text(size = 16))


