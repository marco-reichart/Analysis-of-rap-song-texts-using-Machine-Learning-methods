# -*- coding: utf-8 -*-
"""
Run from project's toplevel directory with
scrapy crawl genius:spider
"""

import scrapy
import logging
import re
from datetime import datetime
import sys, traceback
import json


def parse_int(s, base=10, val=None):
    """
    Hilfsmethode für das fehlertolerante Parsen von Integers.
    :param s:
    :param base:
    :param val:
    :return:
    """
    if s.isdigit():
        return int(s, base)
    else:
        return val


class GeniusSpider(scrapy.Spider):
    """
    Scrapy Spider für Genius
    Die Feld-Namen der Klasse sind größtenteils von scrapy.Spider geerbt und damit vorgegeben.
    Siehe Scrapy Doku https://doc.scrapy.org/en/latest/topics/spiders.html
    """

    # Scrapy: Name des Spiders
    name = 'genius:spider'

    # Scrapy: Nur innerhalb dieser Domain wird gecrawlt
    allowed_domains = ['genius.com']

    # Scrapy: Auf diesen Startseiten wird das Crawlen begonnen
    start_urls = [
        "https://genius.com/tags/deutscher-rap/all",
        "https://genius.com/tags/deutschsprachiger-rap/all"
    ]

    # Scrapy: Steuerung des Log-Levels
    custom_settings = {
        # WARNING, INFO, DEBUG
        'LOG_LEVEL': 'INFO',
    }

    # Übersichtsseiten der Kategorien zeigen normalerweise bei Genius nur Einträge bis zur 51-52 Seite, deswegen werden sicherheitshalber
    # 55 Seiten als maximale Anzahl genutzt
    max_next_pages = 55
    
    # Alle Künstler, deren Alben schon extrahiert wurden. Verhindert doppeltes Betrachten eines Künstlers
    viewed_artists = set()
    
    # Alle Künstler, die in den jeweiligen Kategorien(Deutschsprachiger-Rap und Deutscher-Rap) gefunden wurden
    # Nur diese Künstler werden betrachten, um mögliche Fehler zu umgehen.
    category_artists = set()
    
    # "Künstler", die Genius-Kategorie-Accounts sind, werden ignoriert, da dies nur Playlists von erschienenen Songs sind
    excluded_artists = {"Rap Genius Deutschland", "Screen Genius Deutschland", "Genius Deutschland"}
    
    def parse(self, response):
        """
        Parse-Callback von Scrapy
        Wird standardmäßig beim Besuchen einer Kategorie-Seite aufgerufen
        :param response:
        :return:
        """

        # Extrahiere Links der einzelnen Songs, falls der Künstler nicht ausgeschlossen ist.
        # Set-Objekt wird für Eindeutigkeit verwendet
        song_links = set()
        for song in response.css("a.song_link"):
            # Whitespaces müssen noch extra behandelt werden, da sonst die Künstler nicht richtig verglichen werden können
            category_artist = song.css("span.title_with_artists span.artist_name span.primary_artist_name::text").extract_first().replace(u'\xa0', u' ')
            if(category_artist not in self.excluded_artists):
                self.category_artists.add(category_artist)
                song_links.add(song.css("::attr(href)").extract_first())

        logging.getLogger().info("VISITING " + response.url)

        # Folge den vorher extrahierten Song-Links
        for song_link in song_links:
            logging.getLogger().info("Song link " + song_link)
            yield response.follow(song_link, callback=self.parse_song)
        
        # Extrahiere den Link auf die Nachfolge-Seite ("Next") und folge ihm, sofern noch nicht max_next_pages erreicht ist
        next_link, page_num = GeniusSpider.get_next_link(response)
        if next_link and page_num <= self.max_next_pages:
            logging.getLogger().info("Next link (%d) %s" % (page_num, next_link))
            yield response.follow(next_link, callback=self.parse)

    def parse_song(self, response):
        """
        Parse-Callback für Songs zur Extraktion der Song-Texte
        :param response: Scrapy Response
        :return:
        """
        page_type = response.css("meta[property='og:type']::attr(content)").extract_first()
       
        if page_type == 'music.song':
            try:
                # JSON-Objekt mit allen benötigten Metadaten
                meta_data = response.css("meta[itemprop=page_data]::attr(content)").extract_first()
                json_meta_data = json.loads(meta_data)
                language = json_meta_data["tracking_data"][21]["value"]
                if(language == "de"):   
                    # Künstler bestimmen
                    artist = response.css("a.header_with_cover_art-primary_info-primary_artist::text").extract_first()
                    # Test benötigt, da man sonst nach längerer Suche auf andere Genres/Künstler stößt => Bsp: Helene Fischer
                    if(artist in self.category_artists):
                        url = response.url
                        logging.getLogger().info("URL " + url)
                        # Titel bestimmen
                        title = response.css("h1.header_with_cover_art-primary_info-title::text").extract_first()

                        # Songtext bestimmen
                        html_text = response.css("div.lyrics p").extract_first()
                        song_text_with_comments =  re.sub(r'<[^>]*>','', html_text)
                        song_text = re.sub(r'\[.*\]','', song_text_with_comments)
                        song_text = re.sub('\s+', ' ', song_text)
                        song_text = song_text.strip(' \n')
                     
                        # Album bestimmen
                        album = response.css("a.song_album-info-title::attr(title)").extract_first()
                
                        # Release-Datum bestimmen
                        release = response.xpath("//div[@initial-content-for='track_info']/div/div/span[contains(text(),'Release Date')]/following-sibling::span/text()").extract_first()
                        if(release == None):
                            released_at = "N/A"
                        else:
                            released_at = datetime.strptime(release, "%B %d, %Y").date()
                            # Konvertierung nach string, damit Konvertierung in JSON möglich ist
                            released_at = datetime.strftime(released_at, "%Y-%m-%d")
                
                        # Anzahl der akzeptierten Referenzen
                        count_referents = len(response.css("a.referent[classification=accepted]").extract())
                        
                        # Aufrufe des Songs
                        pageviews = json_meta_data["dfp_kv"][5]["values"][0]
                        
                        # Explizität des Songs
                        is_explicit = json_meta_data["dfp_kv"][4]["values"][0]
                
                        # Liste aller Tags des Songs
                        tags_unfiltered = json_meta_data["song"]["tags"]
                        tags = []
                        for tag in tags_unfiltered:
                            tags.append(tag["name"])
                        tags = ",".join(tags)
                
                        # Anzahl aller Contributor zum Song
                        contributor_count = json_meta_data["song"]["stats"]["contributors"]
              
                        # Featured Künstler bestimmen
                        song_artists = json_meta_data["dmp_data_layer"]["page"]["artists"]
                        if(len(song_artists) > 1):
                            featured_artists = []
                            for featured_artist in song_artists:
                                if(featured_artist != artist):
                                    featured_artists.append(featured_artist)
                            featured_artists = ",".join(featured_artists)
                        else:
                            featured_artists = "N/A"
                
                        # Test, ob der Künstler und seine Alben schon betrachtet wurden
                        if(artist not in self.viewed_artists):
                            artist_page_link = response.css("a.header_with_cover_art-primary_info-primary_artist::attr(href)").extract_first()
                            self.viewed_artists.add(artist)
                            yield response.follow(artist_page_link, callback=self.parse_artist)
                          
                        #Item, welches in die JSON-Datei geschrieben wird
                        item = dict(table_type='genius_song',
                                    title=title,
                                    url=url,
                                    song_text=song_text,
                                    artist=artist,
                                    album=album,
                                    released_at=released_at,
                                    count_referents=count_referents,
                                    pageviews=pageviews,
                                    tags=tags,
                                    contributor_count=contributor_count,
                                    featured_artists=featured_artists,
                                    is_explicit=is_explicit)

                        yield item

            except:
                e = sys.exc_info()
                exc_type, exc_value, exc_traceback = sys.exc_info()
                logging.getLogger().warning("Problems with " + response.url)
                logging.getLogger().warning(exc_value)
    
    def parse_artist(self, response):
        """
        Parse-Callback für Künstler zur Extraktion aller Alben des Künstlers
        :param response: Scrapy Response
        :return:
        """
        url = response.url
        logging.getLogger().info("URL-Künstler " + url)
        show_all_album = response.css("div.u-quarter_top_margin a.full_width_button::attr(href)").extract_first()
        # Überprüfe, ob es einen "Show all albums by ..."-Button gibt
        if(show_all_album != None):
            album_overview = "https://genius.com" + show_all_album
            yield response.follow(album_overview, callback=self.parse_album_overview)
        else:
            albums = response.css("div.thumbnail_grid-grid_element a.vertical_album_card::attr(href)").extract()
            # Öffne alle Alben
            for album in albums:
                yield response.follow(album, callback=self.parse_album)
                
    def parse_album_overview(self, response):
        """
        Parse-Callback für die Übersicht aller Alben, was alle Alben des Künstlers extrahiert
        :param response: Scrapy Response
        :return:
        """
        albums = response.css("ul.album_list li a.album_link::attr(href)").extract()
        # Öffne alle Alben
        for album in albums:
            album_link = "https://genius.com" + album
            yield response.follow(album_link, callback=self.parse_album)
            
    def parse_album(self, response):
        """
        Parse-Callback für ein Album zur Extraktion aller Songs des Albums
        :param response: Scrapy Response
        :return:
        """
        url = response.url
        logging.getLogger().info("URL-Album " + url)
        songs = response.css("div.chart_row-content a.u-display_block::attr(href)").extract()
        for song in songs:
            # Alle Songs, die Instrumentals sind werden entfernt, da diese Songs keinen Text enthalten
            if("instrumental" not in song):
                yield response.follow(song, callback=self.parse_song)
        
    @staticmethod
    def get_next_link(response):
        """
        Extrahiert den Link auf die nächste Seite von einer Überblicksseite,
        z.B. https://genius.com/tags/deutscher-rap/all?page=1

        As the link is generated by JavaScript/JQuery function, it cannot simply be
        extracted but must be constructed from the javascript logic.

        The returned URL only requests the AJAX update to the current page, e.g.
        https://genius.com/Shindy-dreams-lyrics

        :param response
        :return: link auf nächste Seite
        """

        # Extraktion der Onclick-Aktion, z.B. onclick="clickNumPage_72447402('1')"
        next_onclick = response.css("a.next_page::attr(href)").extract_first()

        # wenn es die letzte Seite ist ...
        if not next_onclick or len(next_onclick)==0:
            return None, None
       
        # Aufbau des Links
        next_link = "https://genius.com" + next_onclick
        current_page_number = ''.join(re.findall(r'page=(\d+)', next_onclick))

        return next_link, parse_int(current_page_number)
