# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import json
import logging

class GeniusSpiderPipeline(object):
    def process_item(self, item, spider):
        return item


class JsonWriterPipeline(object):
    """
    This class is a generic writer for JSON line files (one JSON item per line).
    It expects that each item has an attribute with name 'table_type'
    All items of a type are written to a file named <type name>.jl.
    The get_file() method opens resp. creates these files on demand.
    """

    files = dict()

    def get_file(self, table_type):
        """ Lazy opening of files with Singleton pattern
        If requesting get_file('xyz'), a file 'xyz.jl' is opened,
        added to the dictionary and returned
        :param table_type: file name as well as data table type
        :return: file
        """
        if table_type not in self.files:
            filename = table_type + ".jl"
            logging.getLogger().info("Opening file %s." % filename)
            self.files[table_type] = open(table_type + '.jl', 'w')
        return self.files[table_type]

    def open_spider(self, spider):
        """
        Scrapy function, called at start of the spider.
        Not used.
        :param spider:
        :return:
        """
        pass

    def close_spider(self, spider):
        """
        Scrapy function, called at end of the spider.
        Close all files in the files dict.
        :param spider:
        :return:
        """
        for file in self.files.values():
            logging.getLogger().info("Closing file %s." % file.name)
            file.close()

    def process_item(self, item, spider):
        """
        Scrapy function, called for each item.
        Extracts the type from the item, gets/opens the respective file
        and writes the item.
        :param item:
        :param spider:
        :return: item
        """

        if 'table_type' in item:
            file = self.get_file(item['table_type'])
            # remove type from item dict
            item.pop('table_type')
            line = json.dumps(dict(item))
            file.write(line + "\n")
            # logging.getLogger().info("Item written to file %s." % file.name)
        else:
            logging.getLogger().warning("No table_type info - item not saved.")

        return item
