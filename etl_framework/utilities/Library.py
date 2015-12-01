"""class to keep track of stuff"""

class Library(object):
    """keeps track of deleted/undeleted rows, using the analogy of a Library"""

    def __init__(self, available_items, unavailable_items):
        """initializes Library object"""
        self.current_checked_in_items = frozenset(available_items)
        self.current_checked_out_items = frozenset(unavailable_items)
        self.inventory_to_check = set(available_items)
        self.returned_items = set()
        self.reserved_items = set()

    def reserve_item(self, item):
        """adds to reserved_items"""

        #a checked-out book can still be reserved
        self.reserved_items.add(item)

    def check_inventory(self, item):
        """
        returns item if checked-out
        checks item off inventory checklist if checked-in
        """

        #return item if it was checked-out
        if item in self.current_checked_out_items:
            self.return_item(item)

        #else the item might already be in inventory
        else:
            try:
                self.inventory_to_check.remove(item)
            #the item is new and not catalogued yet
            except KeyError:
                pass

    def return_item(self, item):
        """returns item to library"""
        self.returned_items.add(item)

    def get_newly_unavailable_items(self):
        """gets all missing items plus reserved items"""

        #missing_items are items that were not checked off from inventory list
        missing_items = self.inventory_to_check

        return missing_items.union(self.reserved_items - self.current_checked_out_items)

    def get_newly_available_items(self):
        """gets all returned items minus reserved items"""

        return self.returned_items - self.reserved_items
