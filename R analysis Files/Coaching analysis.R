PlayoffResults <- read_excel("PlayoffResults.xlsm")

PlayoffResults <- PlayoffResults %>% 
  mutate(team = case_when(
    team == "LAR" ~ "LA",
    team == "LAX" ~ "LAC",
    team == "JET" ~ "NYJ",
    team == "OAK" ~ "LV",
    T ~ team
  )) %>% 
  # mutate(Value = case_when(
  #   Result == "Superbowl Winner" ~ 1,
  #   Result == "Superbowl Loser" ~ 0.9,
  #   Result == "AFC Champ Loser" ~ 0.8,
  #   Result == "NFC Champ Loser" ~ 0.8,
  #   Result == "AFC Divisional Loser" ~ 0.7,
  #   Result == "NFC Divisional Loser" ~ 0.7,
  #   Result == "AFC Wild Card Loser" ~ 0.5,
  #   Result == "NFC Wild Card Loser" ~ 0.5,
  #   T ~ 0
  # ),
  mutate(NFL_result =  sub("^(AFC |NFC )", "", Result),
         NFL_result = factor(NFL_result,
                             levels = c( "Wild Card Loser","Divisional Loser",
                                         "Champ Loser", "Superbowl Loser",
                                         "Superbowl Winner"),
                             ordered = T),
         Value = as.numeric(NFL_result)+1,
         season = as.double(if_else(Season < 10, paste0('200', Season), paste0('20', Season))))

CWOE <- readRDS("Shiny Apps/WAR_positiongroup/data/CWOE.rds")

FullDataHC <- CWOE %>%
  mutate(predicted = FinalOutput*5) %>% 
  left_join(PlayoffResults, by = c('season', 'posteam' = 'team')) %>% 
  mutate(Value = if_else(is.na(Value) == 1, 0, Value),
         VsExpected = Value - predicted)  %>% 
  group_by(HC) %>% 
  summarise(Count = n(),
            CWOE = mean(CWOE),
            Predicted = mean(predicted),
            Value = mean(Value),
            MaxVs = max(VsExpected),
            MinVs = min(VsExpected),
            VsExpected = mean(VsExpected),
            ComboValue = (VsExpected * 3 + Predicted * 2 + (MinVs/2) + (MaxVs/2) + Value + (CWOE/2))/7.5) %>% 
  arrange(desc(ComboValue))
