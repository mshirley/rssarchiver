require 'rss'
require 'open-uri'
require 'digest/md5' 
require 'rubygems'
require 'mongo'
require 'zlib'
require 'happymapper'

filesource = ARGV[0]
dbhost = "localhost"
dbname = "BlobNet"
dbcoll = "blobs"
    
module OPML
  class Outline
    include HappyMapper
    tag 'outline'
    attribute :title, String
    attribute :text, String
    attribute :type, String
    attribute :xmlUrl, String
    attribute :htmlUrl, String
    has_many :outlines, Outline
  end
end

class Feed
  def initialize(source)
    @source = source
    @problem = false
    @items = []
  end
  def psource
    puts "Source is #{@source}"
  end
  def parse
    begin
      @rss = RSS::Parser.parse(@source, false)
      if @rss.nil? == true
        raise "ERROR_ISNIL"
        @problem = true
      end
    rescue Timeout::Error
      puts "Timeout::ERROR occured"
      @problem = true
    rescue RSS::NotWellFormedError
      puts "Feed not well formed"
      @problem = true
    rescue
      print "Read error occured: ", $!, "\n"
      @problem = true
    end
  end
  def hasproblem
    if @problem
      return true
    else
      return false
    end
  end
  def genitems 
    if !@problem
    @items = []
    @rss.items.each do |feeditem|
      if @rss.respond_to?('channel') 
        @hash = { "title" => feeditem.title, "body" => feeditem.description, "link" => feeditem.link, "feedsource" => @source }
      else
        if feeditem.content.respond_to?('content')
          @hash = { "title" => feeditem.title.content, "body" => feeditem.content.content, "link" => feeditem.link.href_without_base, "feedsource" => @source }
        else
          @hash = { "title" => feeditem.title.content, "body" => feeditem.summary.content, "link" => feeditem.link.href_without_base, "feedsource" => @source } 
        end
      end
      @items.push(@hash)
    end
    return @items
    else
      puts "Skipping"
    end
  end
  def savetodb(dbhost, dbname, dbcoll)
    if !@problem
    begin
      db = Mongo::Connection.new(dbhost).db(dbname)
      coll = db.collection(dbcoll)
      print "#{@source} -- (x = already exists): "
      @items.each do |feeditem|
       if feeditem['title'].nil? then @title = "empty" else @title = feeditem['title'] end
       if feeditem['body'].nil? then @body = "emtpy" else @body = feeditem['body'] end
       if feeditem['link'].nil? then @link = "empty" else @link = feeditem['link'] end
       md5 = Digest::MD5.hexdigest(@title) + ":" + Digest::MD5.hexdigest(@body) + ":" + Digest::MD5.hexdigest(@link)
       if coll.find("md5" => md5).count > 0
         #puts "#{md5} already in db, skipping"
         print "x"
       else
         blob = { "md5" => md5, "title" => @title, "body" => @body, "link" => @link, "source" => @source}
         coll.insert(blob)
         print "."
       end
      end
      print "\n"
    rescue Mongo::ConnectionFailure
      puts "Mongodb not available at #{dbhost}"
      exit
    end
    else
        puts "problem, not saving to db"
    end
  end
end

if filesource.nil?
  source = ["http://feeds.feedburner.com/KitUp"]
else
  xml_string = File.read(filesource)
  sections = OPML::Outline.parse(xml_string)
  source = Array.new()
  for i in 0...sections.length
    source[i] = sections[i].xmlUrl
  end
end

puts "Current list of URLs"
source = source.compact.reject { |s| s.empty? }
source = source.sort_by{rand}

puts source
totalSources = source.length
puts "Total Sources: #{totalSources}"

source.each do |source| 
  feed = Feed.new(source)
  feed.parse
  feed.genitems
  feed.savetodb(dbhost, dbname, dbcoll)
end
