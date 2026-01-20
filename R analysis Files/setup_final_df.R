'''
Big picture want to pull in the long coach list with season, team, name, role

Join on one of the following (create and standardize) depending on role
- HC: Team wins composite
- OC/DC: EPA composite
- Specialist Coordinator: Specialist EPA composite
- Position Coach: Position WAR composite

Then join together with the closeness mapping df and itself again
- Group by coach, season
- Order by heirarchy
- Take the first record by group (captures Kyle Shanahan without duplicates)

Finally create the aggregation function and compute
'''

closeness_mapping_manual <- read_excel("NFL Coaching Scraper/closeness_mapping_manual.xlsx")

costaff_df <- read_csv("NFL Coaching Scraper/costaff_df_v3.csv")

costaff_df <- costaff_df %>% 
  filter(year >= 2011) %>% 
  mutate(from_role = case_when((from_role == 'Coordinator') & 
                                   (from_role_subcategory %in% c('Offensive Coordinator',
                                                            'offensive coordinator',
                                                            'Offensive coordinator',
                                                            'offensive Coordinator')) ~ 
                                   'Offensive Coordinator',
                                 (from_role == 'Coordinator') & 
                                   (from_role_subcategory %in% c('Defensive Coordinator',
                                                            'Defensive coordinator',
                                                            'defensive coordinator',
                                                            'defensive Coordinator')) ~ 
                                   'Defensive Coordinator',
                               from_role == 'Coordinator' ~ 'Specialist Coordinator',
                                 T ~ from_role),
         to_role = case_when((to_role == 'Coordinator') & 
                                 (to_role_subcategory %in% c('Offensive Coordinator',
                                                               'offensive coordinator',
                                                               'Offensive coordinator',
                                                               'offensive Coordinator')) ~ 
                                 'Offensive Coordinator',
                               (to_role == 'Coordinator') & 
                                 (to_role_subcategory %in% c('Defensive Coordinator',
                                                               'Defensive coordinator',
                                                               'defensive coordinator',
                                                               'defensive Coordinator')) ~ 
                                 'Defensive Coordinator',
                             to_role == 'Coordinator' ~ 'Specialist Coordinator',
                               T ~ to_role),
         )



costaff_df <- costaff_df %>% 
  left_join(closeness_mapping_manual, by = c('from_role' = 'role_category.x',
                                             'from_side' = 'side_of_ball.x',
                                             'to_role' = 'role_category.y',
                                             'to_side' = 'side_of_ball.y'))

costaff_df <- costaff_df %>% 
  group_by(year, team, from_coach_id, mapping_id) %>% 
  arrange(desc(hierarchy)) %>% 
  slice_head(n = 1)


nodes <- read_csv("NFL Coaching Scraper/nodes_v3.csv")



nflConverter <- read_xlsx("NFL Conversion Names.xlsm")


nflConverter <-  nflConverter %>% 
  select('Full Name', Code) %>% 
  distinct()


base_df <-  costaff_df %>% 
  left_join(nodes, by = c('from_coach_id' = 'coach_id'))%>% 
  left_join(nodes, by = c('to_coach_id' = 'coach_id')) %>% 
  left_join(nflConverter, by = c('team' = 'Full Name'))

# HC Team Wins composite --------------------------------------------------
PlayoffResults <- read_excel("Football/Data Csv/Misc/PlayoffResults.xlsm")

PlayoffResults <- PlayoffResults %>% 
  mutate(team = case_when(
    team == "LAR" ~ "LA",
    team == "LAX" ~ "LAC",
    team == "JET" ~ "NYJ",
    team == "OAK" ~ "LV",
    T ~ team
  )) %>% 
  mutate(NFL_result =  sub("^(AFC |NFC )", "", Result),
         NFL_result = factor(NFL_result,
                             levels = c( "Wild Card Loser","Divisional Loser",
                                         "Champ Loser", "Superbowl Loser",
                                         "Superbowl Winner"),
                             ordered = T),
         Value = as.numeric(NFL_result)+1,
         season = as.double(if_else(Season < 10, paste0('200', Season), paste0('20', Season))))

CWOE <- readRDS("Shiny Apps/WAR_positiongroup/data/CWOE.rds")

team_wins_composite <- CWOE %>%
  mutate(predicted = FinalOutput*5) %>% 
  left_join(PlayoffResults, by = c('season', 'posteam' = 'team')) %>% 
  mutate(Value = if_else(is.na(Value) == 1, 0, Value),
         VsExpected = Value - predicted,
         coach = HC)  %>% 
  group_by(season, posteam, coach) %>% 
  summarise(CWOE = mean(CWOE),
            Predicted = mean(predicted),
            Value = mean(Value),
            MaxVs = max(VsExpected),
            MinVs = min(VsExpected),
            VsExpected = mean(VsExpected),
            WinsValue = (VsExpected * 3 + Predicted * 2 + Value + CWOE*2)/8) %>% 
  arrange(desc(WinsValue)) %>% 
  group_by() %>% 
  mutate(Value = percent_rank(WinsValue),
         from_role = 'Head Coach',
         to_role = 'Head Coach') %>% 
  select(season, posteam, from_role, to_role, coach, Value)


base_df <- base_df %>% 
  left_join(team_wins_composite, by = c('year' = 'season', 'Code' = 'posteam',
                                        'from_role' = 'from_role',
                                        'coach.x' = 'coach'))%>% 
  left_join(team_wins_composite, by = c('year' = 'season', 'Code' = 'posteam',
                                        'to_role.x' = 'to_role',
                                        'coach.y' = 'coach'))


# Coordinator EPA composite -----------------------------------------------

teams_epa_oe <- readRDS("NFL/teams_epa_oe.rds")


oc_teams_epa_oe <- teams_epa_oe %>% 
  mutate(role = 'Offensive Coordinator',
         Value = (epa_oe.x * 4 + FinEPA.x * 2 + FinPassEPA.x + FinRushEPA.x)/8) %>%
  group_by() %>% 
  mutate(Value = percent_rank(Value)) %>% 
  select(season, posteam, role, Value)



off_passing_teams_epa_oe <- teams_epa_oe %>% 
  mutate(role = 'Specialist Coordinator',
         side_of_ball = 'Offense',
         subcategory = 'Offensive Passing Game Coordinator',
         Value = (pass_epa_oe.x * 2 + FinPassEPA.x)/3) %>%
  group_by() %>% 
  mutate(Value = percent_rank(Value)) %>% 
  select(season, posteam, role, side_of_ball, subcategory, Value)


off_pass_specialist_teams_epa_oe <- teams_epa_oe %>% 
  mutate(role = 'Specialist Coach',
         side_of_ball = 'Both',
         subcategory = 'Pass Game Specialist',
         Value = (pass_epa_oe.x * 2 + FinPassEPA.x)/3) %>%
  group_by() %>% 
  mutate(Value = percent_rank(Value)) %>% 
  select(season, posteam, role, side_of_ball, subcategory, Value)

off_rushing_teams_epa_oe <- teams_epa_oe %>% 
  mutate(role = 'Specialist Coordinator',
         side_of_ball = 'Offense',
         subcategory = 'Offensive Run Game Coordinator',
         Value = (rush_epa_oe.x * 2 + FinRushEPA.x)/3) %>%
  group_by() %>% 
  mutate(Value = percent_rank(Value))%>% 
  select(season, posteam, role, side_of_ball, subcategory, Value)


off_run_specialist_teams_epa_oe <- teams_epa_oe %>% 
  mutate(role = 'Specialist Coordinator',
         side_of_ball = 'Offense',
         subcategory = 'Run Game Specialist',
         Value = (rush_epa_oe.x * 2 + FinRushEPA.x)/3) %>%
  group_by() %>% 
  mutate(Value = percent_rank(Value))%>% 
  select(season, posteam, role, side_of_ball, subcategory, Value)



DC_teams_epa_oe <- teams_epa_oe %>% 
  mutate(role = 'Defensive Coordinator',
         Value = (epa_oe.y * 4 + FinEPA.y * 2 + FinPassEPA.y + FinRushEPA.y)/8) %>%
  group_by() %>% 
  mutate(Value = 1-percent_rank(Value)) %>% 
  select(season, posteam, role, Value)


def_passing_teams_epa_oe <- teams_epa_oe %>% 
  mutate(role = 'Specialist Coordinator',
         side_of_ball = 'Defense',
         subcategory = 'Defensive Passing Game Coordinator',
         Value = (pass_epa_oe.y * 2 + FinPassEPA.y)/3) %>%
  group_by() %>% 
  mutate(Value = 1-percent_rank(Value)) %>% 
  select(season, posteam, role, side_of_ball, subcategory, Value)


def_pass_specialist_teams_epa_oe <- teams_epa_oe %>% 
  mutate(role = 'Specialist Coordinator',
         side_of_ball = 'Defense',
         subcategory = 'Pass Game Specialist',
         Value = (pass_epa_oe.y * 2 + FinPassEPA.y)/3) %>%
  group_by() %>% 
  mutate(Value = 1-percent_rank(Value)) %>% 
  select(season, posteam, role, side_of_ball, subcategory, Value)

def_rushing_teams_epa_oe <- teams_epa_oe %>% 
  mutate(role = 'Specialist Coordinator',
         side_of_ball = 'Defense',
         subcategory = 'Defensive Run Game Coordinator',
         Value = (rush_epa_oe.y * 2 + FinRushEPA.y)/3) %>%
  group_by() %>% 
  mutate(Value = 1-percent_rank(Value))%>% 
  select(season, posteam, role, side_of_ball, subcategory, Value)


def_run_specialist_teams_epa_oe <- teams_epa_oe %>% 
  mutate(role = 'Specialist Coach',
         side_of_ball = 'Both',
         subcategory = 'Run Game Specialist',
         Value = (rush_epa_oe.y * 2 + FinRushEPA.y)/3) %>%
  group_by() %>% 
  mutate(Value = 1-percent_rank(Value))%>% 
  select(season, posteam, role, side_of_ball, subcategory, Value)


#### JOIN TOGETHER ####

base_df2 <- base_df %>% 
  left_join(oc_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                        'from_role.x' = 'role')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(oc_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                        'to_role.x' = 'role')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value) 



base_df3 <- base_df2 %>% 
  left_join(off_passing_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                    'from_role.x' = 'role',
                                    'from_side' = 'side_of_ball',
                                    'from_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(off_passing_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                    'to_role.x' = 'role',
                                    'to_side' = 'side_of_ball',
                                    'to_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value)


base_df4 <- base_df3 %>% 
  left_join(off_pass_specialist_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                             'from_role.x' = 'role',
                                             'from_side' = 'side_of_ball',
                                             'from_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(off_pass_specialist_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                             'to_role.x' = 'role',
                                             'to_side' = 'side_of_ball',
                                             'to_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value)


base_df5 <- base_df4 %>% 
  left_join(off_rushing_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                                     'from_role.x' = 'role',
                                                     'from_side' = 'side_of_ball',
                                                     'from_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(off_rushing_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                                     'to_role.x' = 'role',
                                                     'to_side' = 'side_of_ball',
                                                     'to_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value)


base_df6 <- base_df5 %>% 
  left_join(off_run_specialist_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                                     'from_role.x' = 'role',
                                                     'from_side' = 'side_of_ball',
                                                     'from_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(off_run_specialist_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                                     'to_role.x' = 'role',
                                                     'to_side' = 'side_of_ball',
                                                     'to_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value)


# NOW FOR DEFENSIVE SIDE


base_df2 <- base_df6 %>% 
  left_join(DC_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                    'from_role.x' = 'role')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(DC_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                    'to_role.x' = 'role')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value) 



base_df3 <- base_df2 %>% 
  left_join(def_passing_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                             'from_role.x' = 'role',
                                             'from_side' = 'side_of_ball',
                                             'from_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(def_passing_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                             'to_role.x' = 'role',
                                             'to_side' = 'side_of_ball',
                                             'to_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value)


base_df4 <- base_df3 %>% 
  left_join(def_pass_specialist_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                                     'from_role.x' = 'role',
                                                     'from_side' = 'side_of_ball',
                                                     'from_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(def_pass_specialist_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                                     'to_role.x' = 'role',
                                                     'to_side' = 'side_of_ball',
                                                     'to_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value)


base_df5 <- base_df4 %>% 
  left_join(def_rushing_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                             'from_role.x' = 'role',
                                             'from_side' = 'side_of_ball',
                                             'from_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(def_rushing_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                             'to_role.x' = 'role',
                                             'to_side' = 'side_of_ball',
                                             'to_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value)


base_df6 <- base_df5 %>% 
  left_join(def_run_specialist_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                                    'from_role.x' = 'role',
                                                    'from_side' = 'side_of_ball',
                                                    'from_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(def_run_specialist_teams_epa_oe, by = c('year' = 'season', 'Code' = 'posteam',
                                                    'to_role.x' = 'role',
                                                    'to_side' = 'side_of_ball',
                                                    'to_role_subcategory' = 'subcategory')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value)

backup_base_df <- base_df6


# Positional Values -------------------------------------------------------

position_group_oe <- readRDS("NFL/position_group_oe.rds")

position_group_oe <- position_group_oe %>% 
  group_by(pos_group) %>% 
  mutate(Ratio = 1-percent_rank(Ratio),
         WAR_oe = percent_rank(WAR_oe),
         TeamValue = percent_rank(TeamValue)) %>% 
  mutate(Value = (Ratio * 3 + WAR_oe * 2 + TeamValue)/6) %>% 
  group_by() %>% 
  mutate(Value = percent_rank(Value)) %>% 
  select(season, team_name, pos_group, Value)


base_df_pos <- base_df6 %>% 
  left_join(position_group_oe, by = c('year' = 'season', 'Code' = 'team_name',
                                      'from_position_group' = 'pos_group')) %>% 
  mutate(Value.x = coalesce(Value.x, Value)) %>% 
  select(!Value) %>% 
  left_join(position_group_oe, by = c('year' = 'season', 'Code' = 'team_name',
                                      'to_position_group' = 'pos_group')) %>% 
  mutate(Value.y = coalesce(Value.y, Value)) %>% 
  select(!Value)



# EXPONENTIAL DECAY WEIGHTS -----------------------------------------------

max_year <- 2025
half_life <- 2   # years
lambda <- log(2) / half_life
lambda <- 0.05

base_df_pos <- base_df_pos %>% 
  group_by() %>% 
  mutate(
    exp_decay = exp(-lambda * (max_year - year))
    )



# base_df_pos$Value.x[is.na(base_df_pos$Value.x)] <- 0
# base_df_pos$Value.y[is.na(base_df_pos$Value.y)] <- 0


# Final output ------------------------------------------------------------

personal_value_mapping <- closeness_mapping_manual %>% 
  filter(role_category.y == 'Head Coach') %>% 
  mutate(closeness = case_when(role_category.x == 'Head Coach' ~ 0.8, 
                               T ~ closeness)) %>% 
  select(role_category.x, side_of_ball.x, closeness)



final_output <- base_df_pos %>% 
  left_join(personal_value_mapping, by = c('from_role.x' = 'role_category.x', 
                                           'from_side' = 'side_of_ball.x')) %>% 
  group_by(from_coach_id) %>% 
  arrange(desc(year)) %>% 
  summarise(Name = head(coach.x, 1),
            count = n(),
            last_team = head(team, 1),
            last_year = head(year, 1),
            last_role = head(from_role.x, 1),
            last_subcat = head(from_role_subcategory, 1),
            side_of_ball = head(from_side, 1),
            PersonalValue = sum(Value.x * exp_decay, na.rm = T),
            MeanValue.x = mean(Value.x * exp_decay, na.rm = T),
            MaxValue.x = max(Value.x * exp_decay, na.rm = T),
            OverallValue = sum((
              Value.y * closeness.x * exp_decay
            ), na.rm = T),
            MeanValue.y = mean((
              Value.y * closeness.x * exp_decay
            ), na.rm = T),
            MaxValue.y = max((
              Value.y * closeness.x * exp_decay
            ), na.rm = T)) %>% 
  group_by() %>% 
  mutate(MeanValue.y = percent_rank(MeanValue.y),
            CompositeValue = (MeanValue.y * 2 + MeanValue.x + MaxValue.x * 3)/6
            )

write_rds(final_output, 'COACHING CANDIDATES.csv')

filtered_list <- final_output  %>% 
  filter(count >= 15, last_year >= 2024) %>% 
  arrange(desc(CompositeValue)) %>% 
  mutate(PersonalAverage = MeanValue.x,
         PersonalBest = MaxValue.x,
         NetworkAverage = MeanValue.y)%>% 
  select(Name, last_team, last_role, last_subcat, side_of_ball, PersonalAverage, PersonalBest, NetworkAverage, CompositeValue)


write_rds(filtered_list, 'Filtered COACHING CANDIDATES.csv')

background_check <- base_df_pos %>% 
  mutate(from_coach_id = as.factor(from_coach_id))


ggplot(filtered_list, aes(x = PersonalAverage, y = NetworkAverage))+
  geom_point()




# Next years Value --------------------------------------------------------

year_level <- base_df_pos %>% 
  group_by(from_coach_id, year) %>% 
  summarise(
    Value.x = median(Value.x),
    Value.y = sum(Value.y * closeness),
    .groups = "drop"
  )

season_output <- year_level %>% 
  group_by(from_coach_id) %>% 
  arrange(year, .by_group = TRUE) %>% 
  mutate(
    MeanValue.x = cummean(Value.x),
    MaxValue.x = cummax(Value.x),
    OverallValue = cumsum(Value.y),
    MeanValue.y = cummean(Value.y),
    MaxValue.y = cummax(Value.y),
    NextPersonal = lead(Value.x)
  ) %>% 
  na.omit()
  
  
  model <- lm(formula = NextPersonal ~ Value.x + Value.y + 
                MeanValue.x + MaxValue.x + OverallValue + MeanValue.y + MaxValue.y,
              data = season_output)
  
  print(model)

  season_output$pred <- predict(model)

  season_output$personal_oe <- season_output$NextPersonal-season_output$pred


# Fill out staff ----------------------------------------------------------

  
  staff_search_check <- base_df_pos %>% 
    filter
