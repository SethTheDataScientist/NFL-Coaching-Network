'''
Create an WAR over expected model to pass in for position coaches credit down the line
Need to create versions for each position

Using Years in league, years on team, pos_group average, prior avg as features
'''
library(zoo)

IndivWARData <- readRDS("Shiny Apps/WAR_positiongroup/data/IndivWARData.rds")

IndivWARData <- IndivWARData %>% 
  group_by(player_id) %>% 
  arrange((season)) %>% 
  group_by(player_id, team_name) %>% 
  mutate(years_on_team = seq(1, n(), 1))%>%
  mutate(pos_group = case_when(position == 'CB' |
                                 position == 'S' ~ 'DB',
                               position == 'ED' |
                                 position == 'DI' ~ 'DL',
                               position == 'HB' ~ 'RB',
                               position == 'G' | 
                                 position == 'OC' |
                                 position == 'OT' ~ 'OL',
                               T ~ position)
  )

position_age_war <- IndivWARData %>% 
  group_by(Years, position) %>% 
  summarise(WAR = mean(WAR, na.rm = T))

position_war <- IndivWARData %>% 
  group_by(position) %>% 
  summarise(WAR = mean(WAR, na.rm = T))


teams_df <- IndivWARData %>% 
  group_by() %>% 
  select(season, team_name) %>% 
  distinct()


base_df <- IndivWARData %>% 
  na.omit() %>% 
  group_by(player_id) %>% 
  arrange(desc(season)) %>% 
  mutate(prior_war = rollmean(WAR, k = 3, align = "left", fill = PosAvg)) %>% 
  left_join(position_age_war, by = c('Years', 'position')) %>% 
  select(season, player, team_name, position, pos_group, SideofBall, player_id, Years, years_on_team, WAR.y, prior_war, WAR.x)


# QB ----------------------------------------------------------------------

qb_data <- base_df %>% 
  filter(position == 'QB')

qb_model <- lm(formula = WAR.x ~ Years + years_on_team + WAR.y + prior_war,
            data = qb_data)

qb_pred <- predict(qb_model)

qb_data$war_oe <- qb_data$WAR.x-qb_pred


# wr ----------------------------------------------------------------------

wr_data <- base_df %>% 
  filter(position == 'WR')

wr_model <- lm(formula = WAR.x ~ Years + years_on_team + WAR.y + prior_war,
               data = wr_data)

wr_pred <- predict(wr_model)

wr_data$war_oe <- wr_data$WAR.x-wr_pred


# TE ----------------------------------------------------------------------

TE_data <- base_df %>% 
  filter(position == 'TE')

TE_model <- lm(formula = WAR.x ~ Years + years_on_team + WAR.y + prior_war,
               data = TE_data)

TE_pred <- predict(TE_model)

TE_data$war_oe <- TE_data$WAR.x-TE_pred

# HB ----------------------------------------------------------------------

HB_data <- base_df %>% 
  filter(position == 'HB')

HB_model <- lm(formula = WAR.x ~ Years + years_on_team + WAR.y + prior_war,
               data = HB_data)

HB_pred <- predict(HB_model)

HB_data$war_oe <- HB_data$WAR.x-HB_pred


# OL ----------------------------------------------------------------------

OL_data <- base_df %>% 
  filter(SideofBall == 'OL')

OL_model <- lm(formula = WAR.x ~ Years + years_on_team + WAR.y + prior_war,
               data = OL_data)

OL_pred <- predict(OL_model)

OL_data$war_oe <- OL_data$WAR.x-OL_pred


# DL ----------------------------------------------------------------------

DL_data <- base_df %>% 
  filter(SideofBall == 'DL')

DL_model <- lm(formula = WAR.x ~ Years + years_on_team + WAR.y + prior_war,
               data = DL_data)

DL_pred <- predict(DL_model)

DL_data$war_oe <- DL_data$WAR.x-DL_pred



# DB ----------------------------------------------------------------------

DB_data <- base_df %>% 
  filter(SideofBall == 'DB')

DB_model <- lm(formula = WAR.x ~ Years + years_on_team + WAR.y + prior_war,
               data = DB_data)

DB_pred <- predict(DB_model)

DB_data$war_oe <- DB_data$WAR.x-DB_pred


# LB ----------------------------------------------------------------------

LB_data <- base_df %>% 
  filter(SideofBall == 'LB')

LB_model <- lm(formula = WAR.x ~ Years + years_on_team + WAR.y + prior_war,
               data = LB_data)

LB_pred <- predict(LB_model)

LB_data$war_oe <- LB_data$WAR.x-LB_pred


# Combine outputs ---------------------------------------------------------


ratio_of_best <- IndivWARData %>% 
  filter(Snaps >= 250) %>% 
  group_by(player_id) %>% 
  mutate(MaxWAR = max(WAR),
         Diff = abs(WAR - MaxWAR)
  )  %>% 
  mutate(
    Diff = case_when(
      Years == 1 & season == 2011 ~ Diff * 1.5,
      Years <= 4 ~ ((Years)/4) * Diff,
      T ~ (5/(Years)) * Diff)) %>% 
  group_by(season, team_name, pos_group) %>%
  summarise(LostValue = sum(Diff, na.rm = T),
            TeamValue = sum(WAR, na.rm = T),
            Ratio = abs(LostValue/TeamValue))


player_war_df <- bind_rows(qb_data, wr_data, TE_data, HB_data, OL_data,
                             DL_data, DB_data, LB_data)

position_war_df <- player_war_df %>% 
  group_by(season, team_name, pos_group) %>% 
  summarise(WAR = sum(WAR.x, na.rm = T),
            WAR_oe = sum(war_oe, na.rm = T)) %>% 
  left_join(ratio_of_best, by = c('season', 'team_name', 'pos_group'))


# SAVE DATA ---------------------------------------------------------------


write_rds(position_war_df, 'position_group_oe.rds')
