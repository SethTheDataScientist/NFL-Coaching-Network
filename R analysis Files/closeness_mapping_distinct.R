nfl_staff_updated_OC <- read_csv("/NFL Coaching Scraper/nfl_staff_updated_OC.csv") 
  
  
staff_list <- nfl_staff_updated_OC %>% 
filter(Year >= 2011, role_category %in% c('Head Coach', 'Coordinator', 'Specialist Coach', 'Position Coach - Offense', 'Position Coach - Defense', 'Position Coach - Special Teams')) %>% 
  mutate(role_category = case_when((role_category == 'Coordinator') & 
                    (role_subcategory %in% c('Offensive Coordinator',
                                              'offensive coordinator',
                                              'Offensive coordinator',
                                              'offensive Coordinator')) ~ 
                                      'Offensive Coordinator',
                    (role_category == 'Coordinator') & 
                      (role_subcategory %in% c('Defensive Coordinator',
                                               'Defensive coordinator',
                                               'defensive coordinator',
                                               'defensive Coordinator')) ~ 
                                      'Defensive Coordinator',
                    role_category == 'Coordinator' ~ 'Specialist Coordinator',
                                   T ~ role_category))

distinct_roles <- staff_list %>% 
  select(role_category, side_of_ball) %>% 
  distinct()

closeness_mapping <- distinct_roles %>% 
  cross_join(distinct_roles)

write_csv(closeness_mapping, 'closeness_mapping.csv')

