class TreeViewClass:
    def __init__(
        self,
        databaseObj,
        tree,
        query,
        book_search_flag=False,
        available_days_flag=False,
    ):
        self.databaseObj = databaseObj
        self.tree = tree
        self.query = query
        self.book_search_flag = book_search_flag
        self.available_days_flag = available_days_flag

        self.clear_Treeview()
        self.get_data_from_db()
        self.add_data_to_treeview()

    def clear_Treeview(self):
        
        for i in self.tree.get_children():
            self.tree.delete(i)

    def get_data_from_db(self):
        self.data = self.databaseObj.execute_query(
            query=self.query, get_results=True, get_first_item=False
        )

    def add_data_to_treeview(self):

        if self.book_search_flag == True:
            
            self.data.sort(key=lambda elem: elem[0])
        elif self.available_days_flag == True:
            
            self.data.sort(key=lambda elem: (elem[2] / elem[1]))
        else:
            
            tuple_len = len(self.data[0])

            
            self.data.sort(key=lambda elem: elem[tuple_len - 1], reverse=True)

        for row_tuple in self.data:
            self.tree.insert("", "end", values=row_tuple)
