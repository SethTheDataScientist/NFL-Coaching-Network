'''
Create an EPA over expected modell to pass in for OC/DC credit down the line
Need to create versions for 
- offense
- defense
- passing offense
- passing defense
- rushing offense
- rushing defense

Using QB, roster, prior EPA, and schematic PCs as features
'''
library(zoo)

scheme_df <- read_rds('scheme_df.rds')
EPA_data <- readRDS("Shiny Apps/Season_EPA_Tracker/data/EPA_data.rds")
IndivWARData <- readRDS("Shiny Apps/WAR_positiongroup/data/IndivWARData.rds")

group_war <- IndivWARData %>% 
  group_by(season, team_name, SideofBall) %>% 
  summarise(WAR = sum(WAR, na.rm = T))


position_war <- IndivWARData %>% 
  group_by(season, team_name, position) %>% 
  summarise(WAR = sum(WAR, na.rm = T))

process_data <- function(df, Recency){
  df = df %>% 
    group_by() %>%
    mutate(MaxWeek = case_when(max(Week) > 18 & season >= 2021 ~ 18,
                               max(Week) > 18 ~ 17,
                               T ~ as.double(max(Week))),
           WeeksBack = MaxWeek - Week) %>%
    group_by(posteam, season) %>%
    mutate(AvgFinEPA.x = mean(FinEPA.x),
           FinEPA.xAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinEPA.x,
             (FinEPA.x + (WeeksBack * AvgFinEPA.x))/
               (WeeksBack + 1)),
           
           FinEPA.x = if_else(Recency == "None",  mean(FinEPA.x), mean(FinEPA.xAdj)),
           
           AvgFinPassEPA.x = mean(FinPassEPA.x),
           FinPassEPA.xAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinPassEPA.x,
             (FinPassEPA.x + (WeeksBack * AvgFinPassEPA.x))/
               (WeeksBack + 1)),
           
           FinPassEPA.x = if_else(Recency == "None",  mean(FinPassEPA.x), mean(FinPassEPA.xAdj)),
           
           AvgFinRushEPA.x = mean(FinRushEPA.x),
           FinRushEPA.xAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinRushEPA.x,
             (FinRushEPA.x + (WeeksBack * AvgFinRushEPA.x))/
               (WeeksBack + 1)),
           
           FinRushEPA.x = if_else(Recency == "None",  mean(FinRushEPA.x), mean(FinRushEPA.xAdj)),
           
           AvgFinEPA.y = mean(FinEPA.y),
           FinEPA.yAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinEPA.y,
             (FinEPA.y + (WeeksBack * AvgFinEPA.y))/
               (WeeksBack + 1)),
           
           FinEPA.y = if_else(Recency == "None",  mean(FinEPA.y), mean(FinEPA.yAdj)),
           
           
           AvgFinPassEPA.y = mean(FinPassEPA.y),
           FinPassEPA.yAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinPassEPA.y,
             (FinPassEPA.y + (WeeksBack * AvgFinPassEPA.y))/
               (WeeksBack + 1)),
           
           FinPassEPA.y = if_else(Recency == "None",  mean(FinPassEPA.y), mean(FinPassEPA.yAdj)),
           
           AvgFinRushEPA.y = mean(FinRushEPA.y),
           FinRushEPA.yAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinRushEPA.y,
             (FinRushEPA.y + (WeeksBack * AvgFinRushEPA.y))/
               (WeeksBack + 1)),
           
           FinRushEPA.y = if_else(Recency == "None",  mean(FinRushEPA.y), mean(FinRushEPA.yAdj)),
           
           AvgFinSuccessRate.x = mean(FinSuccessRate.x),
           FinSuccessRate.xAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinSuccessRate.x,
             (FinSuccessRate.x + (WeeksBack * AvgFinSuccessRate.x))/
               (WeeksBack + 1)),
           
           FinSuccessRate.x = if_else(Recency == "None",  mean(FinSuccessRate.x), mean(FinSuccessRate.xAdj)),
           AvgFinPassSuccessRate.x = mean(FinPassSuccessRate.x),
           FinPassSuccessRate.xAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinPassSuccessRate.x,
             (FinPassSuccessRate.x + (WeeksBack * AvgFinPassSuccessRate.x))/
               (WeeksBack + 1)),
           
           FinPassSuccessRate.x = if_else(Recency == "None",  mean(FinPassSuccessRate.x), mean(FinPassSuccessRate.xAdj)),
           
           AvgFinRushSuccessRate.x = mean(FinRushSuccessRate.x),
           FinRushSuccessRate.xAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinRushSuccessRate.x,
             (FinRushSuccessRate.x + (WeeksBack * AvgFinRushSuccessRate.x))/
               (WeeksBack + 1)),
           
           FinRushSuccessRate.x = if_else(Recency == "None",  mean(FinRushSuccessRate.x), mean(FinRushSuccessRate.xAdj)),
           
           
           AvgFinSuccessRate.y = mean(FinSuccessRate.y),
           FinSuccessRate.yAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinSuccessRate.y,
             (FinSuccessRate.y + (WeeksBack * AvgFinSuccessRate.y))/
               (WeeksBack + 1)),
           
           FinSuccessRate.y = if_else(Recency == "None",  mean(FinSuccessRate.y), mean(FinSuccessRate.yAdj)),
           AvgFinPassSuccessRate.y = mean(FinPassSuccessRate.y),
           FinPassSuccessRate.yAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinPassSuccessRate.y,
             (FinPassSuccessRate.y + (WeeksBack * AvgFinPassSuccessRate.y))/
               (WeeksBack + 1)),
           
           FinPassSuccessRate.y = if_else(Recency == "None",  mean(FinPassSuccessRate.y), mean(FinPassSuccessRate.yAdj)),
           
           AvgFinRushSuccessRate.y = mean(FinRushSuccessRate.y),
           FinRushSuccessRate.yAdj = if_else(
             (season >= 2021 & Week > 18) | 
               (season < 2021 & Week > 17), FinRushSuccessRate.y,
             (FinRushSuccessRate.y + (WeeksBack * AvgFinRushSuccessRate.y))/
               (WeeksBack + 1)),
           
           FinRushSuccessRate.y = if_else(Recency == "None",  mean(FinRushSuccessRate.y), mean(FinRushSuccessRate.yAdj)),
           
    ) %>% 
    slice_tail(n = 1)
}


team_epa <- EPA_data %>%
  filter(Type == 'Include all plays')

EPAData <- process_data(team_epa,'Yes')



teams_df <- scheme_df %>% 
  select(season, posteam)

# Offense EPA -------------------------------------------------------------

skill_war <- group_war %>% 
  filter(SideofBall %in% c('SKILL')) 

OL_war <- group_war %>% 
  filter(SideofBall %in% c('OL'))

rb_war <- position_war %>% 
  filter(position %in% c('HB')) 


qb_war <- group_war %>% 
  filter(SideofBall == 'QB') 

  
off_df <- EPAData %>% 
  select(season, posteam, FinEPA.x, FinEPA.y) %>% 
  group_by(posteam) %>% 
  arrange(desc(season)) %>% 
  mutate(prior_epa = rollmean(FinEPA.x, k = 3, align = "left", fill = 0)) %>% 
  left_join(scheme_df, by = c('season', 'posteam')) %>% 
  left_join(OL_war, by = c('season', 'posteam' = 'team_name'))%>% 
  left_join(skill_war, by = c('season', 'posteam' = 'team_name'))%>% 
  left_join(qb_war, by = c('season', 'posteam' = 'team_name'))


model <- lm(formula = FinEPA.x ~ PC1.x + PC2.x +
              WAR.x + WAR.y + WAR + prior_epa,
            data = off_df)

off_df$pred <- predict(model)

off_df$epa_oe.x <- off_df$FinEPA.x-off_df$pred


join_off_df <- off_df %>% 
  select(season, posteam, FinEPA.x, epa_oe.x)

teams_df <- teams_df %>% 
  left_join(join_off_df, by = c('season', 'posteam'))


# Passing Offense EPA -------------------------------------------------------------


off_pass_df <- EPAData %>% 
  select(season, posteam, FinPassEPA.x) %>% 
  group_by(posteam) %>% 
  arrange(desc(season)) %>% 
  mutate(prior_epa = rollmean(FinPassEPA.x, k = 3, align = "left", fill = 0)) %>% 
  left_join(scheme_df, by = c('season', 'posteam')) %>% 
  left_join(skill_war, by = c('season', 'posteam' = 'team_name'))%>% 
  left_join(qb_war, by = c('season', 'posteam' = 'team_name'))


off_pass_model <- lm(formula = FinPassEPA.x ~ PC1.x + PC2.x +
                       WAR.x + WAR.y + prior_epa,
                     data = off_pass_df)

off_pass_df$pred <- predict(off_pass_model)

off_pass_df$pass_epa_oe.x <- off_pass_df$FinPassEPA.x-off_pass_df$pred


join_off_pass_df <- off_pass_df %>% 
  select(season, posteam, FinPassEPA.x, pass_epa_oe.x)

teams_df <- teams_df %>% 
  left_join(join_off_pass_df, by = c('season', 'posteam'))


# rushing Offense EPA -------------------------------------------------------------


off_rush_df <- EPAData %>% 
  select(season, posteam, FinRushEPA.x) %>% 
  group_by(posteam) %>% 
  arrange(desc(season)) %>% 
  mutate(prior_epa = rollmean(FinRushEPA.x, k = 3, align = "left", fill = 0)) %>% 
  left_join(scheme_df, by = c('season', 'posteam')) %>% 
  left_join(rb_war, by = c('season', 'posteam' = 'team_name'))%>% 
  left_join(OL_war, by = c('season', 'posteam' = 'team_name'))


off_rush_model <- lm(formula = FinRushEPA.x ~ PC1.x + PC2.x +
                       WAR.x + WAR.y + prior_epa,
                     data = off_rush_df)

off_rush_df$pred <- predict(off_rush_model)

off_rush_df$rush_epa_oe.x <- off_rush_df$FinRushEPA.x-off_rush_df$pred


join_off_rush_df <- off_rush_df %>% 
  select(season, posteam, FinRushEPA.x, rush_epa_oe.x)

teams_df <- teams_df %>% 
  left_join(join_off_rush_df, by = c('season', 'posteam'))

# defense EPA -------------------------------------------------------------

dl_war <- group_war %>% 
  filter(SideofBall %in% c('DL')) 

db_war <- group_war %>% 
  filter(SideofBall %in% c('DB')) 

lb_war <- group_war %>% 
  filter(SideofBall == 'LB') 


def_df <- EPAData %>% 
  select(season, posteam, FinEPA.x, FinEPA.y) %>% 
  group_by(posteam) %>% 
  arrange(desc(season)) %>% 
  mutate(prior_epa = rollmean(FinEPA.y, k = 3, align = "left", fill = 0)) %>% 
  left_join(scheme_df, by = c('season', 'posteam')) %>% 
  left_join(dl_war, by = c('season', 'posteam' = 'team_name'))%>% 
  left_join(db_war, by = c('season', 'posteam' = 'team_name'))%>% 
  left_join(lb_war, by = c('season', 'posteam' = 'team_name'))


def_model <- lm(formula = FinEPA.y ~ PC1.y + PC2.y +
              WAR.x + WAR.y + WAR + prior_epa,
            data = def_df)

def_df$pred <- predict(def_model)

def_df$epa_oe.y <- def_df$FinEPA.y-def_df$pred

join_def_df <- def_df %>% 
  select(season, posteam, FinEPA.y, epa_oe.y)

teams_df <- teams_df %>% 
  left_join(join_def_df, by = c('season', 'posteam'))


# Passing defense EPA -------------------------------------------------------------


def_pass_df <- EPAData %>% 
  select(season, posteam, FinPassEPA.y) %>% 
  group_by(posteam) %>% 
  arrange(desc(season)) %>% 
  mutate(prior_epa = rollmean(FinPassEPA.y, k = 3, align = "left", fill = 0)) %>% 
  left_join(scheme_df, by = c('season', 'posteam')) %>%  
  left_join(db_war, by = c('season', 'posteam' = 'team_name'))%>% 
  left_join(dl_war, by = c('season', 'posteam' = 'team_name'))


def_pass_model <- lm(formula = FinPassEPA.y ~ PC1.y + PC2.y +
                       WAR.x + WAR.y + prior_epa,
                     data = def_pass_df)

def_pass_df$pred <- predict(def_pass_model)

def_pass_df$pass_epa_oe.y <- def_pass_df$FinPassEPA.y-def_pass_df$pred


join_def_pass_df <- def_pass_df %>% 
  select(season, posteam, FinPassEPA.y, pass_epa_oe.y)

teams_df <- teams_df %>% 
  left_join(join_def_pass_df, by = c('season', 'posteam'))


# rushing defense EPA -------------------------------------------------------------


def_rush_df <- EPAData %>% 
  select(season, posteam, FinRushEPA.y) %>% 
  group_by(posteam) %>% 
  arrange(desc(season)) %>% 
  mutate(prior_epa = rollmean(FinRushEPA.y, k = 3, align = "left", fill = 0)) %>% 
  left_join(scheme_df, by = c('season', 'posteam')) %>% 
  left_join(dl_war, by = c('season', 'posteam' = 'team_name'))%>% 
  left_join(lb_war, by = c('season', 'posteam' = 'team_name'))


def_rush_model <- lm(formula = FinRushEPA.y ~ PC1.y + PC2.y +
                       WAR.x + WAR.y + prior_epa,
                     data = def_rush_df)

def_rush_df$pred <- predict(def_rush_model)

def_rush_df$rush_epa_oe.y <- def_rush_df$FinRushEPA.y-def_rush_df$pred


join_def_rush_df <- def_rush_df %>% 
  select(season, posteam, FinRushEPA.y, rush_epa_oe.y)

teams_df <- teams_df %>% 
  left_join(join_def_rush_df, by = c('season', 'posteam'))



# SAVE DATA ---------------------------------------------------------------


write_rds(teams_df, 'teams_epa_oe.rds')
