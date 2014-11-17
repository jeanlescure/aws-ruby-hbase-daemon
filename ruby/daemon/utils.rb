module HbaseHandlerUtils
  def has_columns?(qhash)
    !qhash[:select][:columns].nil?
  end
  def all_columns?(columns)
    columns[0] == '*'
  end
  def has_column_list?(qhash)
    if has_columns?(qhash)
      if all_columns?(qhash[:select][:columns])
        false
      else
        true
      end
    else
      false
    end
  end
end