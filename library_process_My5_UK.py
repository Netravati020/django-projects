
# -*- coding: utf-8 -*-
"""
objective = extract Movies and Tv Show urls and extracting required datapoints from Movies and Tv Show

Created on Fri April 26 2024

@author: Netravati Madankar

"""
import sys, pandas as pd
from selenium.webdriver.common.by import By

from MediaVOD.library_processor.library_process_base import BaseCrawling
from WISE.wise_crawling_wrapper import Crawling_Wrap_selenium
from WISE.wise_crawling_wrapper import Crawling_wrap_request

class process(BaseCrawling):
    def __init__(self, utils):
        super().__init__(utils)

        # use selenium class from wrapper and initialize_chrome_driver
        self.crawl_wrapper = Crawling_Wrap_selenium(self.utils)
        self.crawl_wrapper.initialize_chrome_driver()
        # put sleep that you observed in website
        self.random_sleep = self.crawl_wrapper.get_random_number(9, 12)
        # use Beautiful soup class
        self.crawl_wrapper_req= Crawling_wrap_request(self.utils)

        # call generic process for calling all functions for crawling
        self.library_genric_process_1(self.movie_url_extracting,self.movie_data_extracting,self.Tv_show_url_extracting,self.Tv_show_data_extracting)

        # close driver
        self.crawl_wrapper.close()

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
        # region xpath of movie url collection

        base_movie_url_xpath = self.utils.xpaths_dict['movie_url']
        movie_urls_collection_xpath = self.utils.xpaths_dict['movie_url_xpath']
        self.movie_url_input_file_name = 'Movies_url'
        accept_cookies_xpath=self.utils.xpaths_dict['accept_cookies']


        # end region
        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, self.movie_url_input_file_name):
                self.logger.info('Movie url collection already Finished')

                return

            Movie_urls = []
            # logger.info function inserting logs in file
            self.logger.info("Starting Movie url collection ")

            self.crawl_wrapper.open_url(base_movie_url_xpath, self.random_sleep)
            self.logger.info('Hit base movie url = ' + base_movie_url_xpath)
            self.crawl_wrapper.accept_cookie(accept_cookies_xpath)

            # collection of genre_urls
            for i in range(0, 10):
                try:
                    self.crawl_wrapper.scroll_with_hight(2000)
                except:
                    pass
            # xpath of movie urls

            for urls in self.crawl_wrapper.find_info("xpath", movie_urls_collection_xpath,
                                                              type_of_element='elements'):
                movie_urls = self.crawl_wrapper.get_href_value(urls)
                if movie_urls == '':
                    continue
                Movie_urls.append(movie_urls)
             # create input xlsx file
            movie_urls_df = self.create_input_xlsx_file(Movie_urls, self.utils.library_filename + self.movie_url_input_file_name+".xlsx")

            self.logger.info('Collected movie urls total : ' + str(len(movie_urls_df['urls'])),
                             Process_id=f'{self.utils.ProcessID}',
                            library_instance=f'{self.utils.library_instance}',
                            Transaction_id=f'{self.utils.TransactionID}')

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, self.movie_url_input_file_name)
            self.logger.info('Xlsx file is generated include movie urls')

            # sending email alert for success of movie url collection
            self.utils.send_email_alert(self.utils.library_name, self.movie_url_input_file_name)
            # self.logger.info('Email alert sent for movie urls is completed')

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.error(f"Exception in movie_url_extracting function: {e}",
                              Process_id=f'{self.utils.ProcessID}',
                            library_instance=f'{self.utils.library_instance}',
                           Transaction_id=f'{self.utils.TransactionID}')
            self.utils.send_email_alert(self.utils.library_name, self.movie_url_input_file_name, e)
            sys.exit()

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

        # region xpath of movie data collection
        movie_synopsis_xpath = self.utils.xpaths_dict['movie_synopsis']
        movie_duration_xpath = self.utils.xpaths_dict['movie_duration']
        movie_rating_xpath = self.utils.xpaths_dict['movie_rating']
        movie_data_output_comon_file_name = 'Movies_data'
        accept_cookies_xpath=self.utils.xpaths_dict['accept_cookies']
        self.movie_url_input_file_name  = 'Movies_url'

        # end region
        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, movie_data_output_comon_file_name):
                self.logger.info('Movie data collection already Finished',
                                 Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')
                return

            self.movie_final_data_list_of_dict = []

            # read input file for movie urls based on the condition.
            movie_input_url = self.input_filter_read_excel_file(excel_filename_path=self.utils.library_filename + self.movie_url_input_file_name)

            # Read columns from config
            movie_columns_str = self.utils.movie_columns
            movie_columns_list = movie_columns_str.split(',')

            # initialize dataframe with fixed column name for movie
            self.df_movies_schema = pd.DataFrame(columns=movie_columns_list)

            self.logger.info('Starting Movie data collection ',
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # loop every movie url for collect datapoints
            for index, row in movie_input_url.iterrows():
                status_of_url = row['status']
                if status_of_url == 'Done':
                    continue
                movie_url = row['urls']

                self.logger.info(f'Collecting movie data for url : {movie_url}',
                                 Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')

                self.crawl_wrapper.open_url(movie_url, self.random_sleep)
                self.crawl_wrapper.accept_cookie(accept_cookies_xpath)
                title = month = day = currency = sdrent = sdbuy = hdrent = hdbuy = rating = mov_format = cast = year = duration = network = synopsis = director = production_company = writer = genre = ""

                soup=self.crawl_wrapper_req.BeautifulSoup_covert(self)
                self.title= soup.find('img', {'class': "lazy-load-image"}).get('alt')

                # self.logger.info(' Title = ' + str(title))
                synopsis = self.crawl_wrapper.find_info("xpath", movie_synopsis_xpath,type_of_element='element')

                duration = self.crawl_wrapper.find_info("xpath", movie_duration_xpath, type_of_element='element').replace('| Movies','').replace('min', '').strip()

                # self.logger.info(' Duration = ' + str(duration))

                rating = self.crawl_wrapper.find_info("xpath", movie_rating_xpath, type_of_element='element')

                # self.logger.info(' Rating = ' + str(rating))

                movie_final_data_dict = {'Content Type': 'Movie', 'Service': self.utils.library_name.split('_')[0],
                                         'Country':  self.utils.library_instance.split('_')[-1],
                                         'Collection Date': self.utils.collectiondate, 'Title': title, 'Year':self.year,
                                         'Month': month, 'Day': day, 'Rating': rating, 'Currency': currency,
                                         'Price SD Rent': sdrent, 'Price SD Buy': sdbuy, 'Price HD Rent': hdrent,
                                         'Price HD Buy': hdbuy, 'Genre': genre, 'Duration (minutes)': duration,
                                         'Network': network, 'Synopsis': synopsis, 'Language': 'English',
                                         'Production': production_company, 'Studio': '', 'Cast': cast,
                                         'Director': director, 'Writer': writer, 'Format': mov_format, 'URL': movie_url}
                # append data in list
                self.movie_final_data_list_of_dict.append(movie_final_data_dict)

                self.logger.info(f'Collected total movie data count : {str(len(self.movie_final_data_list_of_dict))}',
                                 Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')

                # update and save status in input file
                self.update_and_save_excel(index, movie_input_url, "status", "Done",
                                           self.utils.library_filename + self.movie_url_input_file_name)
            # make data file
            self.create_output_xlsx_file(self.df_movies_schema, self.movie_final_data_list_of_dict,
                                         self.utils.library_filename + movie_data_output_comon_file_name)
            # # send email alert for success of movie data collection
            self.utils.send_email_alert(self.utils.library_name, movie_data_output_comon_file_name)
            # self.logger.info('Email alert sent for movie data is completed')

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, movie_data_output_comon_file_name)

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.error(f"Exception in movie_data_extracting function: {e} for url : {movie_url}",
                              Process_id=f'{self.utils.ProcessID}',
                              library_instance=f'{self.utils.library_instance}',
                              Transaction_id=f'{self.utils.TransactionID}')
            # make data file whatever is completed
            self.create_output_xlsx_file(self.df_movies_schema, self.movie_final_data_list_of_dict,
                                         self.utils.library_filename + movie_data_output_comon_file_name)
            self.utils.send_email_alert(self.utils.library_name, movie_data_output_comon_file_name, e)
            sys.exit()

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

        # region xpath of tv show url collection
        base_tv_show_url = self.utils.xpaths_dict['tv_show_url_xpath']
        tv_show_url_collection_xpath = self.utils.xpaths_dict['tv_url_xpath']
        accept_cookies_xpath=self.utils.xpaths_dict['accept_cookies']
        self.tv_show_url_input_file_name = 'Tv_shows_url'

        # end region
        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, self.tv_show_url_input_file_name):
                self.logger.info('Tv show url collection already Finished',
                Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')
                return

            tv_show_urls = []

            # logger.info function inserting logs in file
            self.logger.info('Starting Tv Show url Collection ',
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # base_tv_show_url is where we are find tv show urls

            self.crawl_wrapper.open_url(base_tv_show_url, self.random_sleep)
            self.logger.info('Hit base tv show url = ' + base_tv_show_url)
            self.crawl_wrapper.accept_cookie(accept_cookies_xpath)
            # collection of genre_urls
            for i in range(0, 100):

                try:
                    self.crawl_wrapper.scroll_with_hight(2000)
                except:
                    pass

            for tvshow_url_tag in self.crawl_wrapper.find_info("xpath", tv_show_url_collection_xpath,
                                                               type_of_element='elements'):
                tv_show_url = self.crawl_wrapper.get_href_value(tvshow_url_tag)
                tv_show_urls.append(tv_show_url)
            # creat output file for tv url collection
            tv_show_urls_df = self.create_input_xlsx_file(tv_show_urls,
                                                        self.utils.library_filename + self.tv_show_url_input_file_name+".xlsx")

            self.logger.info(f'Collected total tv show urls total :' + str(len(tv_show_urls_df['urls'])),
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, self.tv_show_url_input_file_name)
            self.utils.send_email_alert(self.utils.library_name, self.tv_show_url_input_file_name)


        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.error(f"Exception in Tv_show_url_extracting function: {e}",
                              Process_id=f'{self.utils.ProcessID}',
                              library_instance=f'{self.utils.library_instance}',
                              Transaction_id=f'{self.utils.TransactionID}')
            # send email alert
            self.utils.send_email_alert(self.utils.library_name, self.tv_show_url_input_file_name, e)
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

        # region file names
        tv_show_data_output_comon_file_name = 'Tv_shows_data'
        # end region

        try:
            # check this function already executed or not by flag
            if self.check_flag(self.utils.library_folder_path, tv_show_data_output_comon_file_name):
                self.logger.info('Tv show data collection already Finished',
                                 Process_id=f'{self.utils.ProcessID}',
                                 library_instance=f'{self.utils.library_instance}',
                                 Transaction_id=f'{self.utils.TransactionID}')
                return
            # append data in list
            self.tv_show_final_data_list_of_dict = []

            # read input file of tv show seasons urls with condition
            series_input_url_df = self.input_filter_read_excel_file(
                excel_filename_path=self.utils.library_filename + self.tv_show_url_input_file_name)
            # Read columns from config
            tvshow_columns_str = self.utils.tvshows_columns
            tvshow_columns_list = tvshow_columns_str.split(',')
            self.logger.info('Starting Tv show data collection ', Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')
            # initialize dataframe with fixed coulmn name for movie
            self.df_tvshows_schema = pd.DataFrame(columns=tvshow_columns_list)

            self.logger.info('Starting Tv show data collection ',
                             Process_id=f'{self.utils.ProcessID}',
                             library_instance=f'{self.utils.library_instance}',
                             Transaction_id=f'{self.utils.TransactionID}')
            for index, row in series_input_url_df.iterrows():
                status_of_url = row['status']
                if status_of_url == 'Done':
                    continue
                # call episode_data function for collect tv show data
                self.episode_data(index, row, series_input_url_df)
                self.logger.info(
                    f'Collected total Tv Show data count : {str(len(self.tv_show_final_data_list_of_dict))}',
                    Process_id=f'{self.utils.ProcessID}',
                    library_instance=f'{self.utils.library_instance}',
                    Transactfion_id=f'{self.utils.TransactionID}')
            # make data file
            self.create_output_xlsx_file(self.df_tvshows_schema, self.tv_show_final_data_list_of_dict,self.utils.library_filename + tv_show_data_output_comon_file_name)

            # set flag for completion of function
            self.set_flag(self.utils.library_folder_path, tv_show_data_output_comon_file_name)

            # send email alert for success of tv show data collection
            self.utils.send_email_alert(self.utils.library_name, tv_show_data_output_comon_file_name)
            # self.logger.info('Email alert sent for tv show data is completed')

        except Exception as e:
            # exception for any error while collecting movie url and send alert
            self.logger.error(f"Exception in Tv_show_data_extracting function: {e} for url {row['urls']}",
                              Process_id=f'{self.utils.ProcessID}',
                              library_instance=f'{self.utils.library_instance}',
                              Transaction_id=f'{self.utils.TransactionID}')
            # make data file whatever is done
            self.create_output_xlsx_file(self.df_tvshows_schema, self.tv_show_final_data_list_of_dict,
                                         self.utils.library_filename + tv_show_data_output_comon_file_name)
            self.utils.send_email_alert(self.utils.library_name, tv_show_data_output_comon_file_name, e)
            sys.exit()

    def episode_data(self, index, row, series_input_url_df):

        # region tv show data xpath
        accept_cookies_xpath = self.utils.xpaths_dict['accept_cookies']
        episode_link_xpath = self.utils.xpaths_dict['episode_link']
        episode_name_xpath = self.utils.xpaths_dict['tv_show_episode_name']
        episode_no_xpath = self.utils.xpaths_dict['tv_show_episode_no']
        tv_show_synopsis_xpath = self.utils.xpaths_dict['tv_show_synopsis']
        tv_show_rating_xpath = self.utils.xpaths_dict['tv_show_rating']
        tv_show_season_no_xpath = self.utils.xpaths_dict['tv_show_season_no']
        tv_show_duration_xpath = self.utils.xpaths_dict['tv_show_duration']
        seasons_xpath = self.utils.xpaths_dict['season_xpath']
        # end region

        tv_show_url = row['urls']

        self.crawl_wrapper.open_url(tv_show_url)
        self.crawl_wrapper.accept_cookie(accept_cookies_xpath)
        self.title = self.month = self.day = self.year = self.currency = self.sdrent = self.sdbuy = self.hdrent = self.hdbuy = self.cast = self.director = self.genre = self.writer = self.synopsis = self.duration = self.rating = self.season_no = self.show_url = self.Episode_no = self.Episode_name = self.Episode_Synopsis = self.syn = self.no_epi = self.episode_url = ""

        self.logger.info(f'Collecting Tv Show Data for url : {tv_show_url}',
                         Process_id=f'{self.utils.ProcessID}',
                         library_instance=f'{self.utils.library_instance}',
                         Transaction_id=f'{self.utils.TransactionID}')

        ep_links = []
        # get episode links
        for ep_link in self.crawl_wrapper.find_info("xpath", episode_link_xpath, type_of_element='elements'):
            if "show" in self.crawl_wrapper.get_href_value(ep_link):
                pass
            else:
                ep_links.append(self.crawl_wrapper.get_href_value(ep_link))
        # collect episode details
        episode_details= self.crawl_wrapper.driver.find_elements('xpath',seasons_xpath )
        for epi_detail in episode_details:

            if "Season" in epi_detail.text:
                pass
            else:
                self.Episode_Synopsis=epi_detail.find_element(By.CSS_SELECTOR, "p").text

                self.Episode_name=epi_detail.find_element("xpath",episode_name_xpath).text
                try:
                    self.Episode_no=epi_detail.find_element("xpath",episode_no_xpath).text
                except:
                    self.Episode_no=""
                self.show_url = self.crawl_wrapper.driver.current_url

                self.title = self.crawl_wrapper.driver.title.split('-')[1].strip()

                self.rating = self.crawl_wrapper.find_info("xpath", tv_show_rating_xpath, type_of_element='element')

                self.syn = self.crawl_wrapper.find_info("xpath", tv_show_synopsis_xpath, type_of_element='element')

                self.season_no = self.crawl_wrapper.find_info("xpath", tv_show_season_no_xpath,type_of_element='element').replace('Season', '').strip()

                dur = self.crawl_wrapper.find_info("xpath", tv_show_duration_xpath, type_of_element='element')
                self.duration = dur.split(' | ')[0].replace(' min', '')
                self.genre = dur.split(' | ')[1]
                tv_show_final_data_dict = {'Content Type': 'Tv Show',
                                           'Service': self.utils.library_instance.split('_')[0],
                                           'Country': self.utils.library_instance.split('_')[-1],
                                           'Collection Date': self.utils.collectiondate, 'Title': self.title,
                                           'Year': self.year, 'Month': '', 'Day': '', 'Season Number': self.season_no,
                                           'Episode Number': self.Episode_no, 'Episode Name': self.Episode_name,
                                           'Number Episodes': '', 'Rating': self.rating, 'Currency': '',
                                           'Price SD Rent': '',
                                           'Price SD Buy': '', 'Price HD Rent': '', 'Price HD Buy': '',
                                           'Genres': self.genre, 'Duration (minutes)': self.duration,
                                           'Network': '', 'Synopsis': self.syn, 'Language': '',
                                           'Production Company': '', 'Studio': '', 'Cast': self.cast,
                                           'Director': self.director, 'Writer': '',
                                           'Format': '', 'Season URL': self.show_url, 'Episode URL': self.episode_url,
                                           'Episode Synopsis': self.Episode_Synopsis}
                # append data in list
                self.tv_show_final_data_list_of_dict.append(tv_show_final_data_dict)
                self.update_and_save_excel(index, series_input_url_df, "status", "Done",
                                           self.utils.library_filename + self.tv_show_url_input_file_name)

        seasn_links_list = []
        season_link_xpath = self.utils.xpaths_dict['season_link']
        # get season url
        for get_seasn_url in self.crawl_wrapper.find_info("xpath", season_link_xpath, type_of_element='elements'):
            links = self.crawl_wrapper.get_href_value(get_seasn_url)
            seasn_links_list.append(links)
        #  open each season urls and collect episode details
        for seasn_links in range(0, len(seasn_links_list)):
            self.crawl_wrapper.open_url(seasn_links_list[seasn_links])
            for ep_link in self.crawl_wrapper.find_info("xpath", episode_link_xpath, type_of_element='elements'):
                if "show" in self.crawl_wrapper.get_href_value(ep_link):
                    pass
                else:
                    ep_links.append(self.crawl_wrapper.get_href_value(ep_link))
            # collect episode details

            episode_details = self.crawl_wrapper.driver.find_elements('xpath',seasons_xpath)
            for epi_detail in episode_details:

                if "Season" in epi_detail.text:
                    pass
                else:
                    self.Episode_Synopsis = epi_detail.find_element(By.CSS_SELECTOR, "p").text

                    self.Episode_name = epi_detail.find_element("xpath", episode_name_xpath).text
                    try:
                        self.Episode_no = epi_detail.find_element("xpath", episode_no_xpath).text
                    except:
                        self.Episode_no=""

                    self.show_url = self.crawl_wrapper.driver.current_url

                    self.title = self.crawl_wrapper.driver.title.split('-')[1].strip()

                    self.rating = self.crawl_wrapper.find_info("xpath", tv_show_rating_xpath, type_of_element='element')

                    self.syn = self.crawl_wrapper.find_info("xpath", tv_show_synopsis_xpath, type_of_element='element')

                    self.season_no = self.crawl_wrapper.find_info("xpath", tv_show_season_no_xpath,type_of_element='element').replace('Season','').strip()

                    dur = self.crawl_wrapper.find_info("xpath", tv_show_duration_xpath, type_of_element='element')
                    self.duration = dur.split(' | ')[0].replace(' min', '')
                    self.genre = dur.split(' | ')[1]
                    tv_show_final_data_dict = {'Content Type': 'Tv Show',
                                               'Service': self.utils.library_instance.split('_')[0],
                                               'Country': self.utils.library_instance.split('_')[-1],
                                               'Collection Date': self.utils.collectiondate, 'Title': self.title,
                                               'Year': self.year, 'Month': '', 'Day': '',
                                               'Season Number': self.season_no,
                                               'Episode Number': self.Episode_no, 'Episode Name': self.Episode_name,
                                               'Number Episodes': '', 'Rating': self.rating, 'Currency': '',
                                               'Price SD Rent': '',
                                               'Price SD Buy': '', 'Price HD Rent': '', 'Price HD Buy': '',
                                               'Genres': self.genre, 'Duration (minutes)': self.duration,
                                               'Network': '', 'Synopsis': self.syn, 'Language': '',
                                               'Production Company': '', 'Studio': '', 'Cast': self.cast,
                                               'Director': self.director, 'Writer': '',
                                               'Format': '', 'Season URL': self.show_url,
                                               'Episode URL': self.episode_url,
                                               'Episode Synopsis': self.Episode_Synopsis}
                    # append data in list
                    self.tv_show_final_data_list_of_dict.append(tv_show_final_data_dict)
                    self.update_and_save_excel(index, series_input_url_df, "status", "Done",
                                               self.utils.library_filename + self.tv_show_url_input_file_name)

