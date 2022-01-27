Dataset to create the commodity tree according to FAO technical report 2011.
Commodities selected are coffe, cocoa, soybeans, pal oil fruit, and cattle.

The file contains 6 columns:

primary_commodity: name of the primary commodity name (just for subsetting purposes)
parent: parent product name, left side of each branch of the trees
child: name of the product derived from the parent, right side of the tree
parent_code: FAO code of the parent
child_code: FAO code of the child
bp: indicator of coproducts or biproducts (e.g., cake and oil). This is used to apply the value share weighing for the calculation of the parent quantity
