import logging
from datamodel.search.FkfungNssaba_datamodel import FkfungNssabaLink, OneFkfungNssabaUnProcessedLink, add_server_copy, get_downloaded_content
from spacetime.client.IApplication import IApplication
from spacetime.client.declarations import Producer, GetterSetter, Getter, ServerTriggers
from lxml import html,etree
import re, os
from time import time
from uuid import uuid4

from urlparse import urlparse, parse_qs
from uuid import uuid4

logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"

@Producer(FkfungNssabaLink)
@GetterSetter(OneFkfungNssabaUnProcessedLink)
@ServerTriggers(add_server_copy, get_downloaded_content)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        self.app_id = "FkfungNssaba"
        self.frame = frame


    def initialize(self):
        self.count = 0
        l = FkfungNssabaLink("http://www.ics.uci.edu/")
        #l = FkfungNssabaLink("http://www.ics.uci.edu/ugrad/polices/academic_standing")
        print l.full_url
        self.frame.add(l)

    def update(self):
        unprocessed_links = self.frame.get(OneFkfungNssabaUnProcessedLink)
        if unprocessed_links:
            link = unprocessed_links[0]
            print "Got a link to download:", link.full_url
            downloaded = link.download()
            links = extract_next_links(downloaded)
            for l in links:
                if is_valid(l):
                    self.frame.add(FkfungNssabaLink(l))

    def shutdown(self):
        print (
            "Time time spent this session: ",
            time() - self.starttime, " seconds.")

def extract_next_links(rawDataObj):
    outputLinks = []
    '''
    rawDataObj is an object of type UrlResponse declared at L20-30
    datamodel/search/server_datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded.
    The frontier takes care of that.

    Suggested library: lxml
    '''
    # print("URL")
    # print(rawDataObj.url)
    # print("CONTENT HII4")
    # text2 = rawDataObj.url
    # text = rawDataObj.content
    # print(type(text))

    siteURL = rawDataObj.url

    if(len(rawDataObj.content) != 0):
        ShtmlContent = html.document_fromstring(rawDataObj.content)
    else:
        return outputLinks

    #Changes base url if the link is is_redirected
    if(rawDataObj.is_redirected):
        siteURL = rawDataObj.final_url
        #outputLinks.append(siteURL)

    # except:
    #print("URL")
    #print(rawDataObj.url)

    #print("\nERROR MSG")
    #print(rawDataObj.error_message)

    #print("\nHTTP_CODE: " + str(rawDataObj.http_code))
    # #
    # #     print("CONTENT")
    # #     print(rawDataObj.content)
    # #
    #
    htmlContent = html.document_fromstring(rawDataObj.content)

    #Convert all relative paths to absolute using siteURL
    htmlContent.make_links_absolute(siteURL, resolve_base_href = True)

    #Find all links (returned in a tuple with other information)
    htmlContentLinks = htmlContent.iterlinks()

    #Iterate through, filter unwanted urls, and add to list
    for link in htmlContentLinks:
        #Filter out for elements with href tags and the url does not have spaces
        if(link[1] == "href" and not " " in link[2]):
            #print(link[2])
            outputLinks.append(link[2].encode('utf-8'))


    return outputLinks

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be
    downloaded or not.
    Robot rules and duplication rules are checked separately.
    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    print("URL: "+ str(url))
    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        return False
