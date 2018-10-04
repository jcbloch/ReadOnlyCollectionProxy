class ReadOnlyCollectionProxy

  include Enumerable

  ###  Why?
  #
  #   while AR querries are fast, at some point 1 ruby object is instantiated for each "row" returned
  #   and that object has it's own children objects-- which means a lot of objects are created, and that slow shit down
  #   OTTH, having access to the aR model's methods is very helpful. If we simply pulled all the data from the db (mysql)
  #   we would get those methods!
  #
  #  so Enter the ReadOnlyCollectionProxy!  Basically you suck all the data down from the db, and shove it into an instance of this proxy
  #  the proxy can then be used with any standard enumarator, and return the AR Model instance all set with the data from the db
  #  However, it does so without creating tons of rails AR objects: indeed it creats only 1
  #
  # to make this even better, we are also going to "redo" all the simple associations too!
  # this could be better, but, well, we are going to simple prefetch the objects, in emdeeded ReadOnlyProxy...

  # so, suppose we are fetching a collection of items which belong to a Customer
  #
  # we could use:   items = Cutomer.find(cutomer_id).items
  # which would return a collections of Items (instances of the Item class)
  #
  # or we could use ReadOnlyCollectionProxy
  #
  # 1: fetch the "sql" for the current db that you would normally use:
  #
  #   sql = Item.limit(1000).to_sql
  #
  # 2: execute that sql query,  but do NOT create AR/AM objects (that is do not parse the result and create instances of Item class)
  #
  #   raw = Item.connection.execute(sql)
  #
  # 3: create the proxy, which will behave more or less like a collection of Item instances
  #    in this case "raw.fields" contains all the fields (columns) retreived from the db
  #    and we are alsi retrieving te customer (but only selected fields)
  #
  #   proxy = ReadOnlyCollectionProxy.new(Item,raw, raw.fields, {includes :[ customer: {select:"id,name"}]})
  #
  #


###  example (msyql adapter)
#
#    sql = Item.limit(1000).to_sql
#       => "SELECT  `items`.* FROM `items`  LIMIT 1000"
#
#    raw = Item.connection.execute(sql)
#        => #<Mysql2::Result:0x007fa83a831208>
#
#   proxy = ReadOnlyCollectionProxy.new(Item,raw,raw.fields)
#
#     => #<ReadOnlyCollectionProxy:0x007fa83a83d4e0 @instance=#<Item id: nil, ...
#
#   proxy.each{|item| puts "#{item.id}\t{item.name}\t{item.some_instance_method_on_item}" }; "ok"
#
# you can also "test" the proxy like this:
#
#  proxy.current.id     [results depends on what was done last, defaults to first row]
#
# and you can step through it..
#  enum = proxy.each
#  proxy.current.id  => first row  (why? defaults to row 0 no matter what)
#  enum.next
#  proxy.current.id  => first row  AGAIN (why?  that's what next does!)
#  enum.next
#  proxy.current.id  => second row
#



  def initialize(klass, data, columns, options={})

    # klass :  AR/AM class
    # data from executing query on db, directly from connector/adapter
    # columns: {method=>index} or  [method,index] or [method,method,nil,method]

    if !columns.respond_to?(:keys) && columns[0].class != Array
      arr = []
      columns.each_with_index{ |k,i| arr.push [k,i] }
    else
      arr = columns
    end

    @fields = data.fields

    @instance = klass.new
    @instance.readonly!
    @data = data_arr = data.to_a

    @proxy_class = klass

    ### all the variables have to be local! or they will dissappear!
    ###  arguably this is a bit-o-trickery.
    ### note: @rocp_row__ must not already be in used in the ARclass

    index= nil
    instance = @instance
    instance.instance_variable_set("@rocp_row__",0)  # so, self.current => 0th row of data set

    arr.each do |column, index|
      @instance.define_singleton_method(column) do
        data_arr[instance.instance_variable_get("@rocp_row__")][index]
      end
    end


    build_associations(options[:includes])

  end

  def build_associations(includes_array)
    @belongs_tos = {}
    return if !includes_array
    # each element of the array might be a symbol OR a hash.
    # not a fan of this; should use rails built in stuff
    includes_array.each do |el|

      case el
      when Symbol, String
        k = el
        v = nil
      when Hash
        k = el.to_a[0][0]
        v = el.to_a[0][1]
      end
      build_belongs_to(k,v) if is_belongs_to?(k)
    end

  end

  def build_belongs_to(k,options={})
    selects = (options && options[:select]) || "*"
    v_str = k.to_s
    reflection = @proxy_class.reflections[v_str]
    ids = pluck1(reflection.foreign_key).uniq
    klass = reflection.class_name.constantize
    ### TODO (JCB): expand this in the future to use a nested proxy for even more speed..
    ### NB: errors are silenced.  maybe this should be changed
    bd =  (klass.select(selects).find(ids) rescue [] ).map{|o|  [o.id, o] }
    instance = @instance
    instance.instance_variable_set("@belongs_to_#{v_str}",bd)
     @instance.define_singleton_method(v_str) do
        arr = instance.instance_variable_get("@belongs_to_#{v_str}")
        fk =  instance.send(reflection.foreign_key)
        arr.assoc(fk).last rescue nil
      end
  end



  def pluck1(field)
    i = @fields.index(field.to_s)
    return nil if !i or i < 0
    @data.map{|row| row[i]}
  end

  def is_belongs_to?(v)
    r = @proxy_class.reflections[v.to_s]
    r && r.macro == :belongs_to
  end

  def current
    @instance
  end

   def each
    return enum_for(:each) unless block_given?
    instance = @instance
    @data.each_with_index do |r, i|
      instance.instance_variable_set("@rocp_row__",i)
      yield instance
    end
  end

  def proxy_class
    @proxy_class
  end




end
