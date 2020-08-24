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
            players.fillna("", inplace=True)
            players = self._set_highlight_hitters(players)
        elif hitterOrPitcher == "pitchers":
            players["Players"] = players["Pitchers"].apply(lambda x:x.split("/")[0] if "/" in x else "")
            players.drop(columns="Pitchers", inplace=True)
            players.fillna("", inplace=True)
            players = self._set_highlight_pitchers(players)
        players_json = json.loads(players.to_json(orient="records"))
        return players_json


    def _set_highlight_hitters(self, df):
        players_df = df.iloc[:-1]
        total_df = df.iloc[-1:]
        for column in players_df.columns:
            if column == "R":
                players_df[column] = players_df[column].apply(
                    lambda x: {"text":x, "highlight":self._get_R_hitters_highlight(x)}
                )
            elif column == "H":
                players_df[column] = players_df[column].apply(
                    lambda x: {"text":x, "highlight":self._get_H_hitters_highlight(x)}
                )
            elif column == "RBI":
                players_df[column] = players_df[column].apply(
                    lambda x: {"text":x, "highlight":self._get_RBI_hitters_highlight(x)}
                )
            elif column == "BB":
                players_df[column] = players_df[column].apply(
                    lambda x: {"text":x, "highlight":self._get_BB_hitters_highlight(x)}
                )
            elif column == "K":
                players_df[column] = players_df[column].apply(
                    lambda x: {"text":x, "highlight":self._get_K_hitters_highlight(x)}
                )
            elif column == "AVG":
                players_df[column] = players_df[column].apply(
                    lambda x:{"text":x, "highlight":self._get_AVG_hitters_highlight(x)}
                )
            else:
                players_df[column] = players_df[column].apply(
                    lambda x:{"text":x, "highlight":""}
                )
        for column in total_df.columns:
            total_df[column] = total_df[column].apply(
                    lambda x:{"text":x, "highlight":""}
                )
        df = pd.concat([players_df, total_df])
        return df

    def _set_highlight_pitchers(self, df):
        players_df = df.iloc[:-1]
        total_df = df.iloc[-1:]
        for column in players_df.columns:
            if column == "ER":
                players_df[column] = players_df[column].apply(
                    lambda x: {"text":x, "highlight":self._get_ER_pitchers_highlight(x)}
                )
            elif column == "H":
                players_df[column] = players_df[column].apply(
                    lambda x: {"text":x, "highlight":self._get_H_pithers_highlight(x)}
                )
            elif column == "BB":
                players_df[column] = players_df[column].apply(
                    lambda x: {"text":x, "highlight":self._get_BB_pitchers_highlight(x)}
                )
            elif column == "K":
                players_df[column] = players_df[column].apply(
                    lambda x: {"text":x, "highlight":self._get_K_pitchers_highlight(x)}
                )
            elif column == "HR":
                players_df[column] = players_df[column].apply(
                    lambda x:{"text":x, "highlight":self._get_HR_pitchers_highlight(x)}
                )
            else:
                players_df[column] = players_df[column].apply(
                    lambda x:{"text":x, "highlight":""}
                )
        for column in total_df.columns:
            total_df[column] = total_df[column].apply(
                    lambda x:{"text":x, "highlight":""}
                )
        df = pd.concat([players_df, total_df])
        return df


    def _get_R_hitters_highlight(self, x):
        if str(x).isdigit():
            if float(x)>3:
                return "good"
        return ""
    
    def _get_ER_pitchers_highlight(self, x):
        if str(x).isdigit():
            if float(x)>3:
                return "bad"
        return ""

    def _get_H_hitters_highlight(self, x):
        if str(x).isdigit():
            if float(x)>5:
                return "good"
        return ""

    def _get_H_pithers_highlight(self, x):
        if str(x).isdigit():
            if float(x)>7:
                return "bad"
        return ""

    def _get_RBI_hitters_highlight(self, x):
        if str(x).isdigit():
            if float(x)>3:
                return "good"
        return ""

    def _get_BB_hitters_highlight(self, x):
        if str(x).isdigit():
            if float(x)>3:
                return "good"
        return ""

    def _get_BB_pitchers_highlight(self, x):
        if str(x).isdigit():
            if float(x) > 6:
                return "bad"
        return ""

    def _get_K_hitters_highlight(self, x):
        if str(x).isdigit():
            if float(x)>3:
                return "bad"
        return ""

    def _get_K_pitchers_highlight(self, x):
        if str(x).isdigit():
            if float(x)>6:
                return "good"
        return ""

    def _get_AVG_hitters_highlight(self, x):
        try:
            if float(x)>0.333:
                return "good"
        except:
            return ""
        return ""

    def _get_HR_pitchers_highlight(self, x):
        if str(x).isdigit():
            if float(x)>=3:
                return "bad"
        return ""


if __name__ == "__main__":
    feed_day = datetime.now() - timedelta(days = 1)
    feeder = MLBFeeder(feed_day)
    # print(feeder._get_game_stats("401226097"))
    data = feeder._get_game_stats("401226097")
    with open("example.json", "w", encoding="utf-8") as file:
        json.dump(data, file)