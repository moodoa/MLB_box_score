import re
import json
import time
import requests
import pandas as pd

from datetime import datetime, timedelta
from bs4 import BeautifulSoup


class MLBFeeder():
    def __init__(self, start_date):
        self.site_url = "http://www.espn.com"
        self.start_date = start_date
        self.team_info = pd.read_csv("MLB_data.csv")
        self.team_info.columns = [
            "full_name",
            "abbreviation",
            "mandarin_name",
            "mandarin_abbr",
            "color1",
            "color2",
        ]
    def _load_team_info(self):
        team_info = pd.read_csv("./MLB_data.csv")
        team_info.columns = [
            "full_name",
            "abbreviation",
            "mandarin_name",
            "mandarin_abbr",
            "color1",
            "color2",
        ]
        return team_info

    def _get_game_ids(self):
        start_date = self.start_date.strftime("%Y%m%d")
        content = requests.get(f"{self.site_url}/mlb/schedule/_/date/{start_date}").content
        soup = BeautifulSoup(content,"html.parser")
        schedule_container = soup.find(name="div", attrs={"id":"sched-container"})
        today_game_id_html = []
        for tag in schedule_container:
            if len(today_game_id_html) != 0 and tag.name == "h2":
                break
            elif tag.attrs == {"class": ["responsive-table-wrap"]}:
                today_game_id_html.append(tag)
        game_ids = []
        for idx in range(len(today_game_id_html)):
            elements = today_game_id_html[idx].select("a")
            for element in elements:
                if "/mlb/game?gameId=" in element.attrs["href"]:
                    game_ids.append(element.attrs["href"].split("=")[1])
        return set(game_ids)
    
    def _is_final(self, game_id):
        try:
            content = requests.get(
                f"{self.site_url}/mlb/boxscore?gameId={game_id}"
            ).content
            soup = BeautifulSoup(content, "html.parser")
            status_detail = soup.find(
                name="span", attrs={"class": "status-detail"}
            ).text
        except:
            return False
        return "Final" in status_detail

    def _get_game_stats(self, game_id):
        game_content = requests.get(f"{self.site_url}/mlb/game?gameId={game_id}").content
        game_soup = BeautifulSoup(game_content, "html.parser")
        box_content = requests.get(f"{self.site_url}/mlb/boxscore?gameId={game_id}").content
        box_soup = BeautifulSoup(box_content, "html.parser")
        
        stat_json = {}
        z_time = game_soup.find(name="div", attrs={"class": "game-date-time"}).span[
            "data-date"
        ]
        stat_json["game_time"] = z_time
        excerpt = self._get_excerpt(box_soup)
        stat_json["excerpt"] ="勝利投手:\n{win_pitcher}\n{win_stat}\n\n敗戰投手:\n{lose_pitcher}\n{lose_stat}".format(
            win_pitcher = excerpt[0],
            win_stat = excerpt[2],
            lose_pitcher = excerpt[1],
            lose_stat = excerpt[3],
        )
        away = self._get_teaminfo_score_streak(0, box_soup, game_soup)
        home = self._get_teaminfo_score_streak(1, box_soup, game_soup)
        
        away.update(
            {
                "hitters": self._get_players_json(box_soup, 0, "hitters"),
                "pitchers": self._get_players_json(box_soup, 1, "pitchers"),
            }
        )
        home.update(
            {
                "hitters": self._get_players_json(box_soup, 2, "hitters"),
                "pitchers": self._get_players_json(box_soup, 3, "pitchers"),
            }
        )
        stat_json["away"] = away
        stat_json["home"] = home

        return stat_json
    
    def _get_excerpt(self, box_soup):
        excerpt_html = box_soup.find(name="div", attrs={"class":"linescore__situation-container"})
        excerpt = []
        for tag in excerpt_html.find_all(name="span", attrs={"class":"fullName"}):
            excerpt.append(tag.text)
        for tag in excerpt_html.find_all(name="span", attrs={"class":"statline"}):
            excerpt.append(tag.text)
        return excerpt

    def _get_teaminfo_score_streak(self, index, box_soup, game_soup):
        html = box_soup.find(name="div", attrs={"class":"responsive-table-wrap"})
        score = pd.read_html(str(html))[0]
        score.drop(columns=score.columns[0], inplace=True)
        team_score = json.loads(score.to_json(orient="records"))
        team_score = team_score[index]
        
        teams_streak = game_soup.find_all(name="div", attrs={"class":"record"})
        streak = teams_streak[index].text.split(",")[0]

        abbrs = box_soup.find_all(name="span", attrs={"class":"abbrev"})
        abbreviation = abbrs[index].text
        team_full_name = self.team_info[self.team_info["abbreviation"] == abbreviation][
            "full_name"
        ].to_list()[0]
        mandarin_name = self.team_info[self.team_info["abbreviation"] == abbreviation][
            "mandarin_name"
        ].to_list()[0]
        mandarin_abbr = self.team_info[self.team_info["abbreviation"] == abbreviation][
            "mandarin_abbr"
        ].to_list()[0]
        color1 = self.team_info[self.team_info["abbreviation"] == abbreviation][
            "color1"
        ].to_list()[0]
        color2 = self.team_info[self.team_info["abbreviation"] == abbreviation][
            "color2"
        ].to_list()[0]

        return {
            "team_score": team_score,
            "abbreviation": abbreviation,
            "team_full_name": team_full_name,
            "mandarin_name": mandarin_name,
            "mandarin_abbr": mandarin_abbr,
            "color1": color1,
            "color2": color2,
            "streak": streak,
        }

    def _get_players_json(self, box_soup, index, hitterOrPitcher):
        content_html = box_soup.find_all(name="div", attrs={"class":"content"})[index]
        for span_stat in content_html.find_all(name="span"):
            span_stat.insert_after("/")
        players = pd.read_html(str(content_html))[0]
        if hitterOrPitcher == "hitters":
            players["Postion"] = players["Hitters"].apply(lambda x:x.split("/")[1] if "/" in x else "")
            players["Players"] = players["Hitters"].apply(lambda x:x.split("/")[0] if "/" in x else "")
            players.drop(columns="Hitters", inplace=True)
        elif hitterOrPitcher == "pitchers":
            players["Players"] = players["Pitchers"].apply(lambda x:x.split("/")[0] if "/" in x else "")
            players.drop(columns="Pitchers", inplace=True)
        players.fillna("", inplace=True)
        players_json = json.loads(players.to_json(orient="records"))
        return players_json


#     def _set_highlight(self, df):
#             players_df = df.iloc[:-2]
#             total_df = df.iloc[-2:]
#             for column in players_df.columns:
#                 if column == "MIN":
#                     players_df[column] = players_df[column].apply(
#                         lambda x: {"text":x, "highlight":"good"} 
#                         if str(x).isdigit() and int(x) > 40 
#                         else {"text":x, "highlight":""}
#                     )
#                 elif column == "Players":
#                     players_df[column] = players_df[column].apply(
#                         lambda x: {"text":x, "highlight":self._is_triple_double(x, players_df)}
#                     )
#                 elif column == "FG":
#                     players_df[column] = players_df[column].apply(
#                         lambda x: {"text":x, "highlight":self._get_fg_highlight(x)}
#                     )
#                 elif column == "3PT":
#                     players_df[column] = players_df[column].apply(
#                         lambda x: {"text":x, "highlight":self._get_3pt_highlight(x)}
#                     )
#                 elif column == "FT":
#                     players_df[column] = players_df[column].apply(
#                         lambda x: {"text":x, "highlight":self._get_ft_highlight(x)}
#                     )
#                 elif column in ["OREB", "DREB", "REB", "AST", "STL", "BLK"]:
#                     players_df[column] = players_df[column].apply(
#                         lambda x: {"text":x, "highlight":self._get_stat_highlight(x)}
#                     )
#                 elif column in ["TO", "PF"]:
#                     players_df[column] = players_df[column].apply(
#                         lambda x: {"text":x, "highlight":self._get_negative_stat_highlight(x)}
#                     )
#                 elif column == "PTS":
#                     players_df[column] = players_df[column].apply(
#                         lambda x: {"text":x, "highlight":self._get_points_highlight(x)}
#                     )
#                 elif column == "+/-":
#                     players_df[column] = players_df[column].apply(
#                         lambda x:{"text":x, "highlight":self._get_plusminus_highlight(x)}
#                     )
#                 elif column in ["DNP", "STARTER"]:
#                     pass
#             for column in total_df.columns:
#                 if column in ["DNP", "STARTER"]:
#                     pass
#                 else:
#                     total_df[column] = total_df[column].apply(
#                         lambda x:{"text":x, "highlight":""}
#                     )
#             df = pd.concat([players_df, total_df])
#             return df

#     def _is_triple_double(self, player, df):
#         positive_stat = []
#         for column in ["REB", "AST", "STL", "BLK", "PTS"]:
#             try:
#                 positive_stat.append(int(df[df["Players"] == player][column].values[0]))
#             except:
#                 positive_stat.append(0)
#         count = 0 
#         for stat in positive_stat:
#             if stat >= 10:
#                 count +=1
#         # 如果未來有大三元需求可在這調整
#         return "" if count >= 3 else ""
        
#     def _get_fg_highlight(self, shoot_made_attempted):
#         if self._is_percentage(shoot_made_attempted):
#             made = int(shoot_made_attempted.split("-")[0])
#             attempted = int(shoot_made_attempted.split("-")[1])
#             if made/attempted >= 0.7:
#                 return "good"
#             elif made/attempted < 0.3:
#                 return "bad"
#         return ""

#     def _get_3pt_highlight(self, shoot_made_attempted):
#         if self._is_percentage(shoot_made_attempted):
#             made = int(shoot_made_attempted.split("-")[0])
#             attempted = int(shoot_made_attempted.split("-")[1])
#             if made/attempted >= 0.5:
#                 return "good"
#             elif made/attempted <= 0.25:
#                 return "bad"
#         return ""

#     def _get_ft_highlight(self, shoot_made_attempted):
#         if self._is_percentage(shoot_made_attempted):
#             made = int(shoot_made_attempted.split("-")[0])
#             attempted = int(shoot_made_attempted.split("-")[1])
#             if made/attempted >= 0.9:
#                 return "good"
#             elif made/attempted <= 0.5:
#                 return "bad"
#         return ""

#     def _is_percentage(self, shoot_made_attempted):
#         attempted = 0
#         if shoot_made_attempted[0].isdigit() and "-" in shoot_made_attempted:
#             attempted = int(shoot_made_attempted.split("-")[1])
#         return True if attempted > 0 else False

#     def _get_stat_highlight(self, stat):
#         if self.is_number(str(stat)):
#             if float(stat) >= 10:
#                 return "good"
#         return ""
    
#     def _get_negative_stat_highlight(self, stat):
#         if self.is_number(str(stat)):
#             if float(stat) == 0:
#                 return "good"
#             elif float(stat) >= 6:
#                 return "bad"
#         return ""

#     def _get_plusminus_highlight(self, stat):
#         if self.is_number(str(stat)):
#             if float(stat) >= 20:
#                 return "good"
#             elif float(stat) <=-20:
#                 return "bad"
#         return ""

#     def _get_points_highlight(self, points):
#         if self.is_number(str(points)):
#             if float(points) >= 30:
#                 return "good"
#         return ""

#     def is_number(self, num):
#         pattern = re.compile(r'^[-+]?[-0-9]\d*\.\d*|[-+]?\.?[0-9]\d*$')
#         result = pattern.match(num)
#         return True if result else False
if __name__ == "__main__":
    feed_day = datetime.now() - timedelta(days = 1)
    feeder = MLBFeeder(feed_day)
    data = feeder._get_game_stats("401226097")
    with open("example.json", "w", encoding="utf-8") as file:
        json.dump(data, file)