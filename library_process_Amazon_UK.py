# -*- coding: utf-8 -*-
"""
objective = extract Movies and Tv Show urls and extracting required datapoints from Movies and Tv Show

Created on March 28 2024

@author: Netravati

"""
import sys, pandas as pd
import re, time
from MediaVOD.library_processor.library_process_base import BaseCrawling
from WISE.wise_crawling_wrapper import Crawling_Wrap_selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException

class process(BaseCrawling):
    def __init__(self, utils):
        super().__init__(utils)

        # use selenium class from wrapper and initialize_chrome_driver
        self.crawl_wrapper = Crawling_Wrap_selenium(self.utils)
        self.crawl_wrapper.initialize_chrome_driver()
        # put sleep that you observed in website
        self.random_sleep = self.crawl_wrapper.get_random_number(7, 9)

        # call generic process for calling all functions for crawling
        self.library_genric_process_1(self.movie_url_extracting, self.movie_data_extracting, self.Tv_show_url_extracting, self.Tv_show_data_extracting)
        # close driver
        self.crawl_wrapper.close()

    # function to collect Movie urls
    def movie_url_extracting(self):
        """
           Collects movie URLs from a specified base URL and genre URLs.

           This method performs the following steps:
           1. Checks if movie URL extraction has already been completed by checking a flag.
           2. Gathers genre URLs from the base movie URL.
           3. Iterates through genre URLs to collect movie URLs.
           4. Stores the collected movie URLs in a DataFrame, removes duplicates, and saves to an Excel file.
           5. Sends an email alert upon successful completion.

           Raises:
               Exception: If any error occurs during the movie URL collection process.

           Note:
               This method relies on certain configurations specified in the 'url_path' section of the config.
        """
        content_click_xpath= self.utils.xpaths_dict['movie_content_click']
        see_more_xpath = self.utils.xpaths_dict['see_more']
        accept_cookie_xpath= self.utils.xpaths_dict['accept_cookie']
        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, 'movie_url_flag.txt'):
                # self.logger.info('Movie url collection already Finished')
                return

            categorey_urls = []
            final_data_movie_url=[]
            # logger.info function inserting logs in file

            self.logger.info('Movie url collection Process started')

            # base_movie_url is where we are find movies urls
            base_movie_url = self.utils.xpaths_dict['movie_url']
            self.crawl_wrapper.open_url(base_movie_url, self.random_sleep)
            self.logger.info('Hit base movie url = ' + base_movie_url)
            # accept cookie button
            self.crawl_wrapper.accept_cookie(accept_cookie_xpath)

            # collection of genre_urls
            self.logger.info('Start gather genre urls')

            # xpath of getting genre urls
            movie_genre_url_xpath = self.utils.xpaths_dict['genre_xpath']
            for categorey in self.crawl_wrapper.find_info("xpath", movie_genre_url_xpath,
                                                              type_of_element='elements'):
                # append one by one category url
                Genre_url=self.crawl_wrapper.get_href_value(categorey)

                if Genre_url == '':
                    continue
                categorey_urls.append(Genre_url)
                self.logger.info(
                    'collected these genre url = ' + Genre_url)

            # collection of movie urls from genre urls
            for Genre_url in categorey_urls:
                self.crawl_wrapper.open_url(Genre_url)
                self.logger.info(
                    'Hit this genre url  = ' + str(Genre_url))

                # xpath of movie urls
                try:
                    movie_content_type=self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", content_click_xpath,type_of_element='elements')[0])
                    self.logger.info(
                        'clicked  = ' + str(movie_content_type))
                except:
                    continue
                all_genre_urls_list = []
                # collect move url from see more section

                all_genre_urls = self.crawl_wrapper.find_info("xpath",see_more_xpath,
                                                              type_of_element='elements')
                for gen_url in all_genre_urls:
                    all_genre_urls_list.append(self.crawl_wrapper.get_href_value(gen_url))

                for all_gen in all_genre_urls_list:
                    self.crawl_wrapper.open_url(all_gen, self.random_sleep)
                    self.logger.info(
                        'Hit this genre url  = ' + str(all_gen))
                    self.crawl_wrapper.one_time_scroll()

                    # Check if the page height has changed (indicating new content)
                    new_page_height = self.crawl_wrapper.driver.execute_script(
                        "return Math.max( document.body.scrollHeight, document.body.offsetHeight, "
                        "document.documentElement.clientHeight, document.documentElement.scrollHeight, "
                        "document.documentElement.offsetHeight);")

                    updated_x = new_page_height - 1000
                    updated_y = new_page_height

                    totalloop = new_page_height // 1000

                    for i in range(totalloop):
                        self.crawl_wrapper.driver.execute_script("window.scrollTo(" + str(updated_x) + "," + str(updated_y) + ")")
                        time.sleep(3)
                        updated_x -= 1000
                        updated_y -= 1000
                        contents_xpath = self.utils.xpaths_dict['movie_contents']
                        Contents = self.crawl_wrapper.find_info("xpath",contents_xpath,type_of_element='elements')
                        self.logger.info(
                            'movie in this section  = ' ,len(Contents))
                        for content in Contents:
                            try:
                                movie_url = content.find_element("xpath",self.utils.xpaths_dict['movie_name']).get_attribute('href')

                                if movie_url == '':
                                    continue
                            except:
                                continue
                            self.logger.info(
                                'Find this movie url = ' + movie_url)
                            final_data_movie_url.append(movie_url)
          # create input xlsx file
            movie_urls_df = self.create_input_xlsx_file(final_data_movie_url, self.utils.library_filename + 'movies_link.xlsx')

            self.logger.info('Total movie urls we got is= ' + str(len(movie_urls_df['urls'])),
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')
            self.logger.info('Xlsx file is generated include movie urls')

            # sending email alert for success of movie url collection
            self.utils.send_email_alert(self.utils.library_name, 'Movie_Urls')
            self.logger.info('Email alert sent for movie urls is completed')

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, 'movie_url_flag.txt')

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.error(f"Exception in movie_url_extracting function: {e}", Process_id=f'{self.utils.ProcessID}',
                              library_instance=f'{self.utils.library_instance}',
                              Transaction_id=f'{self.utils.TransactionID}')
            self.utils.send_email_alert(self.utils.library_name, 'Movie_Urls', e)
            sys.exit()
    # function to collect movie data
    def movie_data_extracting(self):

        """
            Extracts data for movies from a list of URLs.

            This method performs the following steps:
            1. Checks if movie data extraction has already been completed by checking a flag.
            2. Reads movie URLs from an Excel file.
            3. Iterates through each movie URL to scrape relevant data points.
            4. Constructs a DataFrame with the scraped data.
            5. Appends the data to the DataFrame and updates the status in the input Excel file.
            6. Sends an email alert upon successful completion.
            7. Saves the final DataFrame to an Excel file.
            8. Calls the 'Tv_season_url_extracting' function.

            Raises:
                Exception: If any error occurs during the movie data collection process.

            Note:
                This function relies on certain configurations specified in the 'movie_data_xpath' and 'DataFrameColumns' section of the config.
                It uses the 'get_title_with_retry' method for retrieving movie titles and methods from the 'media_core' class
                for logging, email alerts, and flag management.
            """
        Episode_Table_xpath=self.utils.xpaths_dict['episode_table']
        title_xpath=self.utils.xpaths_dict['movie_title']
        imdb_rating_xpath=self.utils.xpaths_dict['imdb_rating']
        movie_rating_xpath=self.utils.xpaths_dict['movie_rating']
        duration_xpath=self.utils.xpaths_dict['duration']
        movie_year_xpath=self.utils.xpaths_dict['movie_year']
        movie_synopsis_xpath=self.utils.xpaths_dict['movie_synopsis']
        PrimeDescription_xpath=self.utils.xpaths_dict['PrimeDescription']
        genre_block=self.utils.xpaths_dict['Genre_Block']
        details_xpath=self.utils.xpaths_dict['details']
        movie_director_xpath=self.utils.xpaths_dict['movie_director']
        languageXPath=self.utils.xpaths_dict['LanguageXPath']
        studioXPath=self.utils.xpaths_dict['StudioXPath']
        dvdPriceXPath=self.utils.xpaths_dict['DVDPriceXPath']
        blurayPriceXPath=self.utils.xpaths_dict['BlurayPriceXPath']
        price_hd_buy_xpath=self.utils.xpaths_dict['price_hd_buy']
        price_sd_buy_xpath= self.utils.xpaths_dict['price_sd_buy']
        price_hd_rent=self.utils.xpaths_dict['price-hd_rent']
        price_sd_rent_xpath= self.utils.xpaths_dict['price_sd_rent']
        accept_cookie_xpath= self.utils.xpaths_dict['accept_cookie']
        close_more_perchase_button_xpath= self.utils.xpaths_dict['close_more_perchase']
        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, 'movie_data_flag.txt'):
                self.logger.info('Movie data collection already Finished')
                return

            self.movie_final_data_list_of_dict = []

            # read input file for movie urls based on the condition.
            movie_input_url = self.input_filter_read_excel_file("status", "Not Done",
                                                                self.utils.library_filename + 'movies_link.xlsx')

            # Read columns from config
            movie_columns_str = self.utils.movie_columns
            movie_columns_list = movie_columns_str.split(',')

            # initialize dataframe with fixed column name for movie
            self.df_movies_schema = pd.DataFrame(columns=movie_columns_list)

            self.logger.info('Movie data Process started')

            # loop every movie url for collect datapoints
            for index, row in movie_input_url.iterrows():
                # logger.info function inserting logs in file

                movie_url = row['urls']
                self.crawl_wrapper.open_url(movie_url, self.random_sleep)
                # click accept cookie button
                self.crawl_wrapper.accept_cookie(accept_cookie_xpath)

                title = month = day = currency = Price_HD_Buy = Price_SD_Rent = Price_SD_Buy = Price_HD_Rent =movie_rating = mov_format = cast = year = duration = network = synopsis =director = production_company = writer = Genre =multiformat = network = ""

                self.logger.info(
                    'hit url no.= ' + str(index) + ' = ' + str(movie_url))
                try:
                    Episode_Table = self.crawl_wrapper.find_info("xpath", Episode_Table_xpath, type_of_element='element')
                    if "Seasons" in Episode_Table:
                        continue
                    if "Season" in Episode_Table:
                        continue
                except:
                    pass
                time.sleep(2)

                title = self.crawl_wrapper.find_info("xpath", title_xpath,
                                                              type_of_element='element')
                self.logger.info('title = ' + str(title))

                imdb_rating = self.crawl_wrapper.find_info("xpath", imdb_rating_xpath, type_of_element='element').replace('IMDb', '').strip()

                movie_rating = self.crawl_wrapper.find_info("xpath", movie_rating_xpath,type_of_element='element')

                duration=self.crawl_wrapper.find_info("xpath",duration_xpath,type_of_element='element')
                duration = duration.replace('min', '').strip()
                if ' h' in duration:
                    duration = duration.replace(' h', '').strip().split(' ')
                    duration = int(duration[0]) * 60 + int(duration[1])

                year = self.crawl_wrapper.find_info("xpath", movie_year_xpath, type_of_element='element')

                synopsis = self.crawl_wrapper.find_info("xpath", movie_synopsis_xpath,type_of_element='element')

                PrimeDescription = self.crawl_wrapper.find_info("xpath",PrimeDescription_xpath,type_of_element='element')

                Genre_list = []
                Genre_Block = self.crawl_wrapper.find_info("xpath", genre_block,type_of_element='elements')
                for k in range(1, len(Genre_Block) + 1):
                    Genre1 = self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict['genre'].format(k),type_of_element='element')
                    if '·' in Genre1:
                        continue
                    Genre_list.append(Genre1)
                Genre = ' | '.join(Genre_list).strip(" . |")
                time.sleep(2)

                # click button for more purchase options
                More_Purchase_options = self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict["MorePurchaseOptions"],type_of_element="elements")[0])
                self.logger.info('clicked button More perchase options')

                Price_HD_Rent = \
                self.crawl_wrapper.find_info("xpath", price_hd_rent, type_of_element="element").split('HD')[-1].strip()

                Price_SD_Rent = \
                self.crawl_wrapper.find_info("xpath", price_sd_rent_xpath, type_of_element="element").split('SD')[-1].strip()

                Price_HD_Buy = \
                self.crawl_wrapper.find_info("xpath", price_hd_buy_xpath, type_of_element="element").split('HD')[-1].strip()

                Price_SD_Buy = \
                self.crawl_wrapper.find_info("xpath", price_sd_buy_xpath, type_of_element="element").split('SD')[-1].strip()

                # close More perchase button
                close_purchase_button = self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", close_more_perchase_button_xpath,type_of_element="elements")[0])
                self.logger.info('closed more perchase button')

                # click on details
                details = self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", details_xpath,type_of_element='elements')[0])
                self.logger.info('clicked section deatils')

                Director_list = []
                Director_Block = self.crawl_wrapper.find_info("xpath",movie_director_xpath,type_of_element='elements')
                for i in range(1, len(Director_Block) + 1):
                    Director1 = self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict["movie_director"].format(i),type_of_element='element')
                    Director_list.append(Director1)
                director = ' | '.join(Director_list)

                Cast_list = []
                Cast_Block = self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict["CastBlock"],type_of_element='elements')
                for j in range(1, len(Cast_Block) + 1):
                    Cast1 = self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict["CastXPath"].format(j),type_of_element='element')
                    Cast_list.append(Cast1)
                cast = ' | '.join(Cast_list)

                language = self.crawl_wrapper.find_info("xpath", languageXPath,type_of_element='element').replace(",", " | ")

                Producer_list = []
                Producer_Block = self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict['ProducerBlock'],type_of_element='elements')
                for m in range(1, len(Producer_Block) + 1):
                    Producer1 =self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict['ProducerXPath'].format(m),type_of_element='element')
                    Producer_list.append(Producer1)
                Producer = ' | '.join(Producer_list)

                studio = self.crawl_wrapper.find_info("xpath", studioXPath,type_of_element='element')

                DVD_price = self.crawl_wrapper.find_info("xpath", dvdPriceXPath,type_of_element='element')
                DVD_price=DVD_price.split('from')[-1].strip()

                Bluray_price = self.crawl_wrapper.find_info("xpath", blurayPriceXPath,type_of_element='element')
                Bluray_price=Bluray_price.split('\n')[-1].strip()

                movie_final_data_dict = {'Content Type': 'Movie', 'Service': self.utils.library_name.split('_')[0],
                                         'Country': self.utils.library_name.split('_')[1],'Collection Date': self.utils.collectiondate, 'Title': title, 'Year': year,
                                         'Month': month, 'Day': day, 'Rating': movie_rating,'Currency': currency,
                                         'Price SD Rent': Price_SD_Rent, 'Price SD Buy': Price_SD_Buy,'Price HD Rent': Price_HD_Rent,
                                         'Price HD Buy': Price_HD_Buy, 'Genre': Genre, 'Duration (minutes)': duration,
                                         'Network': network, 'Synopsis': synopsis, 'Language': language,
                                         'Production': production_company, 'Studio': studio, 'Cast': cast,
                                         'Director': director, 'Writer': writer, 'Format': mov_format,'URL': movie_url,
                                         'Prime_description': PrimeDescription, 'DVD_price': DVD_price,'multiformat_price': multiformat,
                                         'Bluray_price': Bluray_price, 'IMDB_Rating': imdb_rating, 'Producer': Producer}
                # append data in list
                self.movie_final_data_list_of_dict.append(movie_final_data_dict)
                # self.movie_final_data_list_of_dict = pd.concat([self.movie_final_data_list_of_dict, pd.DataFrame([movie_final_data_dict])], ignore_index=True)

                # update and save status in input file
                self.update_and_save_excel(index, movie_input_url, "status", "Done",
                                           self.utils.library_filename + 'movies_data.xlsx')
            # make data file
            self.create_output_xlsx_file(self.df_movies_schema, self.movie_final_data_list_of_dict,
                                         self.utils.library_filename + 'movies_Data.xlsx')
            self.logger.info('Total movie data extraction completed ',
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')
            self.logger.info('Xlsx file is generated include movie data')
            # send email alert for success of movie data collection
            self.utils.send_email_alert(self.utils.library_name, 'Movie_data')
            self.logger.info('Email alert sent for movie data is completed')

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, 'movie_data_flag.txt')

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.exception(f"Exception in movie_data_extracting function: {e}")
            # make data file whatever is completed
            self.create_output_xlsx_file(self.df_movies_schema, self.movie_final_data_list_of_dict,
                                         self.utils.library_filename + 'movies_Data.xlsx')
            self.utils.send_email_alert(self.utils.library_name, 'Movie_data', e)
            sys.exit()

     # function to collect Tv show urls
    def Tv_show_url_extracting(self):

        """
            Extracts TV show URLs and season URLs from a base TV show URL.

            This method performs the following steps:
            1. Checks if TV show URL extraction has already been completed by checking a flag.
            2. Gathers genre URLs for TV shows from the base TV show URL.
            3. Iterates through each genre URL to collect TV show URLs.
            4. Iterates through each TV show URL to collect season URLs.
            5. Removes duplicate TV show URLs and saves the season URLs to an Excel file.
            6. Sends an email alert upon successful completion.
            7. Calls the 'Tv_show_data_extracting' function.

            Raises:
                Exception: If any error occurs during the TV show URL and season URL collection process.

            Note:
                This function relies on certain configurations specified in the 'url_path' section of the config.
                It uses methods from the 'media_core' class for logging, email alerts, and flag management.
            """

        content_click_xpath = self.utils.xpaths_dict['tv_content_click']
        see_more_xpath = self.utils.xpaths_dict['see_more']
        accept_cookie_xpath= self.utils.xpaths_dict['accept_cookie']

        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, 'tv_show_url_flag.txt'):
                self.logger.info('tv show url collection already Finished')
                return

            categorey_urls = []
            final_data_Tv_url = []
            # logger.info function inserting logs in file

            self.logger.info('tv show url collection Process started')

            # base_movie_url is where we are find movies urls
            base_movie_url = self.utils.xpaths_dict['tv_url']
            tv_show_genre_url_xpath = self.utils.xpaths_dict['genre_xpath']

            self.crawl_wrapper.open_url(base_movie_url, self.random_sleep)
            self.logger.info('Hit base tv show url = ' + base_movie_url)
           # click accept cookie button
            self.crawl_wrapper.accept_cookie(accept_cookie_xpath)

            # collection of genre_urls

            self.logger.info('Start gather genre urls')

            # xpath of getting genre urls
            for categorey in self.crawl_wrapper.find_info("xpath", tv_show_genre_url_xpath,
                                                          type_of_element='elements'):
                # append one by one category url
                Genre_url = self.crawl_wrapper.get_href_value(categorey)

                if Genre_url == '':
                    continue
                categorey_urls.append(Genre_url)
                self.logger.info(
                    'collected these genre url = ' + Genre_url)

            # collection of movie urls from genre urls
            for Genre_url in categorey_urls:
                self.crawl_wrapper.open_url(Genre_url)
                self.logger.info(
                    'Hit this genre url  = ' + str(Genre_url))

                # xpath of movie urls
                try:
                    tv_content_type = self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", content_click_xpath,
                                                                      type_of_element='elements')[0])
                    self.logger.info(
                        'clicked  = ' + str(tv_content_type))
                except:
                    continue
                all_genre_urls_list = []
                all_genre_urls = self.crawl_wrapper.find_info("xpath", see_more_xpath,
                                                              type_of_element='elements')
                for gen_url in all_genre_urls:
                    all_genre_urls_list.append(self.crawl_wrapper.get_href_value(gen_url))

                for all_gen in all_genre_urls_list:
                    self.crawl_wrapper.open_url(all_gen, self.random_sleep)
                    self.logger.info(
                        'Hit this genre url  = ' + str(all_gen))
                    self.crawl_wrapper.one_time_scroll()

                    # Check if the page height has changed (indicating new content)
                    new_page_height = self.crawl_wrapper.driver.execute_script(
                        "return Math.max( document.body.scrollHeight, document.body.offsetHeight, "
                        "document.documentElement.clientHeight, document.documentElement.scrollHeight, "
                        "document.documentElement.offsetHeight);")

                    updated_x = new_page_height - 1000
                    updated_y = new_page_height

                    totalloop = new_page_height // 1000


                    for i in range(totalloop):
                        self.crawl_wrapper.driver.execute_script(
                            "window.scrollTo(" + str(updated_x) + "," + str(updated_y) + ")")
                        time.sleep(3)
                        updated_x -= 1000
                        updated_y -= 1000
                        contents_xpath = self.utils.xpaths_dict['tv_show_content']
                        Contents = self.crawl_wrapper.find_info("xpath",contents_xpath,type_of_element='elements')

                        self.logger.info(
                            'movie in this section  = ', len(Contents))
                        for content in Contents:
                            try:
                                movie_url = content.find_element("xpath", self.utils.xpaths_dict['movie_name']).get_attribute('href')
                                if movie_url == '':
                                    continue
                            except:
                                continue
                            self.logger.info(
                                'Find this movie url = ' + movie_url)
                            final_data_Tv_url.append(movie_url)
            # create input xlsx file
            tv_show_urls_df = self.create_input_xlsx_file(final_data_Tv_url, self.utils.library_filename + 'tv_show_link.xlsx')

            self.logger.info('Total movie urls we got is= ' + str(len(tv_show_urls_df['urls'])))

            self.logger.info('Xlsx file is generated include tv_show urls')
            self.logger.info('Total tv urls extraction completed ',
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')
            # sending email alert for success of movie url collection
            self.utils.send_email_alert(self.utils.library_name, 'tv_show_Urls')
            self.logger.info('Email alert sent for tv show urls is completed')

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, 'tv_show_url_flag.txt')

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.error(f"Exception in movie_url_extracting function: {e}")
            self.utils.send_email_alert(self.utils.library_name, 'tv_show_Urls', e)
            sys.exit()
    def Tv_show_data_extracting(self):
        """
            Extracts data for TV show episodes from given TV show season URLs.

            This method performs the following steps:
            1. Checks if TV show data extraction has already been completed by checking a flag.
            2. Maximizes the window of the web driver.
            3. Reads input data from an Excel file containing TV show season URLs.
            4. Reads columns from the configuration file.
            5. Initializes a dataframe with fixed column names for TV show data.
            6. Loops through each TV show season URL to scrape data points for each episode.
            7. Sends email alerts upon successful completion.
            8. Saves the TV show data to an Excel file.
            9. Sets a flag indicating that TV show data extraction has been completed.

            Raises:
                Exception: If any error occurs during the TV show data collection process.

            Note:
                This function relies on certain configurations specified in the 'tv_show_data_xpath' and 'DataFrameColumns' sections of the config.
                It uses methods from the 'media_core' class for logging, email alerts, and flag management.
            """
        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, 'tv_show_data_flag.txt'):
                self.logger.info('Tv show data collection already Finished')
                return
            # append data in list
            self.tv_show_final_data_list_of_dict = []
            self.row_num = 1
            # read input file of tv show seasons urls with condition
            series_input_url_df = self.input_filter_read_excel_file("status", "Not Done",
                                                                    self.utils.library_filename + 'tv_show_link.xlsx')
            tvshow_columns_str = self.utils.tvshows_columns
            tvshow_columns_list = tvshow_columns_str.split(',')

            # initialize dataframe with fixed coulmn name for movie
            self.df_tvshows_schema = pd.DataFrame(columns=tvshow_columns_list)

            self.logger.info('Tv show data Process started')
            self.tv_show_names = []

            xpath_title = self.utils.xpaths_dict['tv_title']
            season_button_xpath = self.utils.xpaths_dict['season_button']
            seasons_xpath = self.utils.xpaths_dict['seasons_xpath']
            seasons_list_path = self.utils.xpaths_dict['seasons_list']
            accept_cookie_xpath = self.utils.xpaths_dict['accept_cookie']

            # loop every season url
            for index, row in series_input_url_df.iterrows():
                series_url = row['urls']
                self.crawl_wrapper.open_url(series_url, self.random_sleep)
                # click accept button
                self.crawl_wrapper.accept_cookie(accept_cookie_xpath)

                # wait until element present
                WebDriverWait(self.crawl_wrapper.driver, 10).until(EC.presence_of_element_located((By.XPATH, xpath_title)))
                title = self.crawl_wrapper.find_info("xpath", xpath_title, type_of_element='element')
                if title == '':
                    try:
                        title = self.utils.xpaths_dict['tv_title2'].get_attribute('alt')
                    except Exception as e:
                        title = ""
                if title not in self.tv_show_names:
                    self.tv_show_names.append(title)
                    # Check the season avaiable
                    if self.crawl_wrapper.find_info("xpath", season_button_xpath, type_of_element='element'):
                       self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", season_button_xpath, type_of_element='elements')[0])
                       time.sleep(2)
                       season_list =  self.crawl_wrapper.find_info("xpath", seasons_xpath, type_of_element='elements')

                       for i in range(1, len(season_list)+1):
                           self.crawl_wrapper.refresh()
                           self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", season_button_xpath, type_of_element='elements')[0])
                           if self.crawl_wrapper.find_info("xpath", seasons_list_path, type_of_element='element'):
                              self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", seasons_list_path.format(str(i)), type_of_element='elements')[0])
                              time.sleep(2)
                              # call feteching episode data function
                              self.fetch_episode_data(series_url, self.row_num, title)
                    else:
                        # Feteching episode data
                        self.fetch_episode_data(series_url, self.row_num, title)
                else:
                    pass


            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, 'tv_show_data_flag.txt')

            # send email alert for success of tv show data collection
            self.utils.send_email_alert(self.utils.library_name, '_tv_show_Data_')
            self.logger.info(
                'Email alert sent for tv show data is completed')

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.exception(f"Exception in Tv_show_data_extracting function: {e}")
            # make data file whatever is done
            self.create_output_xlsx_file(self.df_tvshows_schema, self.tv_show_final_data_list_of_dict,
                                         self.utils.library_filename + 'tv_show_Data.xlsx')
            self.utils.send_email_alert(self.utils.library_name, '_tv_show_Data_', e)
            sys.exit()

    # function to check multiple xpaths
    def check_exists_by_xpath(self, xpath):
        try:
            self.crawl_wrapper.driver.find_element_by_xpath(xpath)
        except NoSuchElementException:
            return False
        return True
    # function takes arguments series urls, header and title
    def fetch_episode_data(self, series_url, row_num, title):
        season_xpath = self.utils.xpaths_dict['season_xpath']
        season_xpath2= self.utils.xpaths_dict['season_xpath2']
        year_xpath = self.utils.xpaths_dict['year']
        Details_xpath = self.utils.xpaths_dict['details']
        sub_data_set_xpath = self.utils.xpaths_dict['sub_data_set']
        episode_click_xpath = self.utils.xpaths_dict['episode_click']
        episode_list_xpath = self.utils.xpaths_dict['episode_list']
        episode_name_xpath = self.utils.xpaths_dict['episode_name']
        Episode_Synopsis_xpath = self.utils.xpaths_dict['Episode_Synopsis']
        prime_Description_xpath = self.utils.xpaths_dict['prime_Description']
        synopsis_xpath = self.utils.xpaths_dict['synopsis']
        rating_xpath = self.utils.xpaths_dict['rating']
        rating_xpath1 = self.utils.xpaths_dict['rating1']
        releasedate_xpath =self.utils.xpaths_dict['releasedate']
        duration_xpath = self.utils.xpaths_dict['tv_show_duration']
        Genre_Block_xpath = self.utils.xpaths_dict['Genre_Block']
        Genre1_xpath = self.utils.xpaths_dict['Genre1']
        #  check season have multiple seasons or single season
        try:
            # below xpath checks multiple season
            season = self.crawl_wrapper.find_info("xpath", season_xpath, type_of_element='element')
            if "season" in season.lower():
                season = str(re.search(r'\d+', season).group())
            elif self.check_exists_by_xpath(season_xpath2):
                season = self.crawl_wrapper.find_info("xpath", season_xpath2, type_of_element='element')
                season = str(re.search(r'\d+', season).group())

        except NoSuchElementException:
            try:
                season = self.crawl_wrapper.find_info("xpath", season_xpath2, type_of_element='element')
                if "season" in season.lower():
                    season = str(re.search(r'\d+', season).group())
                elif self.crawl_wrapper.find_info("xpath", season_xpath2,
                                                  type_of_element='element'):
                    season = self.crawl_wrapper.find_info("xpath", season_xpath2,
                                                          type_of_element='element')
                    season = str(re.search(r'\d+', season).group())

            except NoSuchElementException:
                pass
        year = self.crawl_wrapper.find_info("xpath", year_xpath, type_of_element='element')

        # create empty string
        self.episode_click =self.details =self.lang =self.director = self.starring = self.studio = self.network = ''
        # click details section to get details
        Details=self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", Details_xpath, type_of_element='elements')[0])
        self.logger.info('clicked section Details')

        time.sleep(2)
        sub_data_set=self.crawl_wrapper.find_info("xpath", sub_data_set_xpath, type_of_element='elements')
        # loop to fetch data language, network,director,cast and studio
        for x in range(0, len(sub_data_set)):
            sub = sub_data_set[x]
            sub_data = sub.find_element_by_xpath('.//dt')
            if ("lang" in sub_data.text.lower()):
                lang = sub.find_element_by_xpath('.//dd')
                lang = lang.text
                lang = re.sub(",", "|", lang)
            elif ("irector" in sub_data.text.lower()):
                director = sub.find_element_by_xpath('.//dd')
                director = director.text
                director = re.sub(",", "|", director)
            elif ("etwork" in sub_data.text.lower()):
                network = sub.find_element_by_xpath('.//dd')
                network = network.text
                network = re.sub(",", "|", network)
            elif ("tarring" in sub_data.text.lower()):
                starring = sub.find_element_by_xpath('.//dd').text
                starring = re.sub(",", "|", starring)

            elif ("tudio" in sub_data.text.lower()):
                studio = sub.find_element_by_xpath('.//dd').text
                studio = re.sub(",", "|", studio)
            else:
                pass


        #  click episode section to get episode details
        time.sleep(2)
        episode_click=self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath",episode_click_xpath,type_of_element='elements')[0])
        self.logger.info('clicked section episode click')

        time.sleep(2)
        # take length of episode
        episode_count = episode_list_xpath
        for j in range(0, len(episode_count)):
            time.sleep(2)
            #  collect language, director, starring, studio and network
            self.df_tvshows_schema.loc[row_num,"Language"] = lang
            self.df_tvshows_schema.loc[row_num,"Director"] = director
            self.df_tvshows_schema.loc[row_num,"Cast"] = starring
            self.df_tvshows_schema.loc[row_num,"Studio"] = studio
            self.df_tvshows_schema.loc[row_num,"Network"] = network
            self.df_tvshows_schema.loc[row_num,"Number Episodes"] = len(episode_count)

            # collect episode details
            episode_name=self.crawl_wrapper.find_info("xpath",episode_name_xpath.format(j),type_of_element='element')
            episode_no = str(episode_name.split(" - ")[0].split(" ")[1])
            self.df_tvshows_schema.loc[row_num,"Episode Number"] = episode_no
            episode_title = str(episode_name.split(" - ")[1])
            self.df_tvshows_schema.loc[row_num,"Title"]= episode_title

            Episode_Synopsis=self.crawl_wrapper.find_info("xpath",Episode_Synopsis_xpath.format(j),type_of_element='element')
            self.df_tvshows_schema.loc[row_num,"Episode Synopsis"]= Episode_Synopsis

            prime_Description=self.crawl_wrapper.find_info("xpath",prime_Description_xpath.format(j),type_of_element='element')
            self.df_tvshows_schema.loc[row_num,"Prime_Description"]= prime_Description

            time.sleep(2)
            synopsis=self.crawl_wrapper.find_info("xpath",synopsis_xpath,type_of_element='element')
            self.df_tvshows_schema.loc[row_num, "Synopsis"] = synopsis

            rating=self.crawl_wrapper.find_info("xpath",rating_xpath.format(j-1),type_of_element='element')
            self.df_tvshows_schema.loc[row_num, "Rating"] = rating

            self.crawl_wrapper.find_info("xpath", rating_xpath1, type_of_element='element')
            self.df_tvshows_schema.loc[row_num,"Rating"]= rating

            releasedate=self.crawl_wrapper.find_info("xpath",releasedate_xpath.format(j),type_of_element='element')
            self.df_tvshows_schema.loc[row_num,"Releasedate"]= releasedate

            try:
                duration=self.crawl_wrapper.find_info("xpath",duration_xpath.format(j+1),type_of_element='element')

                self.df_tvshows_schema.loc[row_num, "Duration (minutes)"] = duration.replace('min', '')
            except:
                self.df_tvshows_schema.loc[row_num,"Duration (minutes)"] = ''

            # to collect all genre

            Genre_list = []
            Genre_Block=self.crawl_wrapper.find_info("xpath",Genre_Block_xpath,type_of_element='elements')
            for k in range(1, len(Genre_Block) + 1):
                Genre1=self.crawl_wrapper.find_info("xpath",Genre1_xpath.format(k),type_of_element='element')
                if '·' in Genre1:
                    continue
                Genre_list.append(Genre1)
            Genre = ' | '.join(Genre_list).strip(" . |")

            # click more perchase button to collect pricing options(price hd rent,price hd buy, price sd rent,price sd buy)
            try:
                if self.check_exists_by_xpath(self.utils.xpaths_dict["av_episodes"].format(j)):
                    self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict["av_episodes"], type_of_element='elements')[0])
                    self.crawl_wrapper.wait_for_element("xpath", self.utils.xpaths_dict["pricing_count"])
                    pricing_options = self.crawl_wrapper.find_info("xpath",self.utils.xpaths_dict["pricing_count"], type_of_element='element')
                    self.df_tvshows_schema.loc[row_num, "USD"] = "USD"
                    # loop on lenght of pricing options

                    for x in range(1, len(pricing_options) + 1):
                        value_xpath = self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict["pricing_count_button"].format(x),type_of_element='element')
                        value=self.crawl_wrapper.find_info("xpath",value_xpath.format(x),type_of_element='element')

                        if ("rent" in value.text.lower()):
                            if ("HD" in value.text):
                                self.df_tvshows_schema.loc[row_num,"Price HD Rent"].value = str(value.text.split("$")[1])
                            elif ("SD" in value.text):
                                self.df_tvshows_schema.loc[row_num, "Price SD Rent"] = str(value.text.split("$")[1])
                        elif ("buy" in value.text.lower()):
                            if ("HD" in value.text):
                                self.df_tvshows_schema.loc[row_num, "Price HD Rent"] = str(value.text.split("$")[1])
                            elif ("SD" in value.text):
                                self.df_tvshows_schema.loc[row_num, "Price SD Buy"] = str(value.text.split("$")[1])
                    self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict["model_close"],type_of_element='elements')[0])

                # if more perchase option button is unavailable executes below lines
                elif self.check_exists_by_xpath(self.utils.xpaths_dict["av_episodes2"].format(j)):
                    value = self.crawl_wrapper.find_info("xpath",self.utils.xpaths_dict["av_episodes2"].format(j), type_of_element='element')
                    self.df_tvshows_schema.loc[row_num, "USD"] = "USD"
                    if ("rent" in value.text.lower()):
                        if ("HD" in value.text):
                            self.df_tvshows_schema.loc[row_num, "Price HD Rent"] = str(value.text.split("$")[1])
                        elif ("SD" in value.text):
                            self.df_tvshows_schema.loc[row_num,"Price SD Rent"] = str(value.text.split("$")[1])
                    elif ("buy" in value.text.lower()):
                        if ("HD" in value.text):
                            self.df_tvshows_schema.loc[row_num, "Price HD Buy"] = str(value.text.split("$")[1])
                        elif ("SD" in value.text):
                            self.df_tvshows_schema.loc[row_num,"Price SD Buy"]= str(value.text.split("$")[1])
                else:
                    pass
            # close the model
            except Exception as ex:
                 self.crawl_wrapper.click(self.crawl_wrapper.find_info("xpath", self.utils.xpaths_dict["model_close"],
                                                                          type_of_element='elements')[0])
            # collect below datapoints
            self.df_tvshows_schema.loc[row_num,"Content Type"]= "TV Show"
            self.df_tvshows_schema.loc[row_num, "Service"] = "Amazon"
            self.df_tvshows_schema.loc[row_num, "Country"] = "US"
            self.df_tvshows_schema.loc[row_num, "Season URL"] = series_url
            self.df_tvshows_schema.loc[row_num, "Episode URL"] = self.crawl_wrapper.driver.current_url
            self.df_tvshows_schema.loc[row_num,"Collection Date"]= str(time.strftime("%m/%d/%Y"))

            self.df_tvshows_schema.loc[row_num, "Title"] = title

            self.df_tvshows_schema.loc[row_num, "Year"] = year

            self.df_tvshows_schema.loc[row_num,"Season Number"].value = season

            self.df_tvshows_schema.loc[row_num, "Number Episodes"] = str(len(episode_count))
            # creats headers in dataframe from row one
            # row_num is row
            row_num +=1
            self.row_num = row_num
            tv_final_data_dict = self.df_tvshows_schema.to_dict(orient='records')
            # appended the dictionary to a list
            self.tv_show_final_data_list_of_dict.append(tv_final_data_dict)
            
            # save the data to an Excel file using pandas
            self.df_tvshows_schema.to_excel("tv_shows_data.xlsx", index=False)
            # logs created when execution finished Tv shows data
            self.logger.info('Total Tv data extraction completed ',
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            self.logger.info('Xlsx file is generated include Tv data')
            # sending email alert
            self.utils.send_email_alert(self.utils.library_name, '_tv_show_Data_done')
            self.logger.info('Email alert sent for tv show data extraction done')