package BTree;

public class BTreeTest {
	public static void main(String[] args) {
		BTree<Integer, Integer> tree = new BTree();
		//unique keys
		tree.insert(50, 50);
		tree.insert(15, 15);
		tree.insert(18, 18);
		tree.insert(20, 20);
		tree.insert(21, 21);
		tree.insert(31, 31);
		tree.insert(45, 45);
		tree.insert(47, 47);
		tree.insert(52, 52);
		tree.insert(30, 30);
		tree.insert(19, 19);
		tree.insert(22, 22);

		//duplicate keys
		tree.insert(18, 17);
		tree.insert(20, 20);
		tree.insert(31, 31);
		tree.insert(45, 45);

		tree.delete(18, 17);
		// Initialize the iterator and print all values in the B+ tree
		DBBTreeIterator<Integer, Integer> iterator = new DBBTreeIterator(tree);
		iterator.print();
	}
}
