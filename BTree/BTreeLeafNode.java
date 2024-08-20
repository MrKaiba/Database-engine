package BTree;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mohamed
 */
class BTreeLeafNode<TKey extends Comparable<TKey>, TValue> extends BTreeNode<TKey> {
	protected final static int LEAFORDER = 4;
	/**
	 * @uml.property name="values"
	 */
	private final List<TValue>[] values;

	/**
	 * @uml.property name="filters"
	 * @uml.associationEnd multiplicity="(0 -1)" elementType="java.lang.Boolean"
	 */
	private ArrayList<Boolean> filters;

	private BTreeLeafNode<TKey, TValue> next;

	public BTreeLeafNode() {
		this.filters = new ArrayList<>();
		this.keys = new Object[LEAFORDER + 1];
		this.values = new ArrayList[LEAFORDER + 1];
		for (int i = 0; i < this.values.length; i++) {
			this.values[i] = new ArrayList<>();
		}
		this.next = null;
	}


	public BTreeLeafNode(BTreeLeafNode smallest) {
		this.keys = smallest.keys;
		this.values = smallest.values;
		this.next = null;
	}

	public BTreeLeafNode<TKey, TValue> getNext() {
		return this.next;
	}

	public void setNext(BTreeLeafNode<TKey, TValue> next) {
		this.next = next;
	}

	public List<TValue> getValue(int index) {
		return this.values[index];
	}

	public void setValue(int index, TValue value) {
		List<TValue> valueList = (List<TValue>) this.values[index];
		if (valueList == null) {
			valueList = new ArrayList<>();
			this.values[index] = valueList;
		}
		valueList.add(value);
	}

	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}

	@Override
	public int search(TKey key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			int cmp = this.getKey(i).compareTo(key);
			if (cmp == 0) {
				return i;
			} else if (cmp > 0) {
				return -1;
			}
		}

		return -1;
	}

	@Override
	public String toString() {
		String out = "";
		for (int index = 0; index < this.getKeyCount(); ++index) {
			out += this.getValue(index).toString() + " ";
		}
		return out;
	}

	/* The codes below are used to support insertion operation */

	public void insertKey(TKey key, TValue value) {
		int index = 0;
		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
			++index;
		this.insertAt(index, key, value);
	}

	private void insertAt(int index, TKey key, TValue value) {
		// If the key already exists, add the value to the existing list
		if (index < this.getKeyCount() && this.getKey(index).compareTo(key) == 0) {
			this.values[index].add(value);
			return;
		}

		// Move space for the new key
		for (int i = this.getKeyCount() - 1; i >= index; --i) {
			this.setKey(i + 1, this.getKey(i));
			this.values[i + 1] = this.values[i];
		}

		// Insert new key and value
		this.setKey(index, key);
		this.values[index] = new ArrayList<>();
		this.values[index].add(value);
		++this.keyCount;
	}

	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed
	 * to parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {
		int midIndex = this.getKeyCount() / 2;

		BTreeLeafNode<TKey, TValue> newRNode = new BTreeLeafNode<TKey, TValue>();

		for (int i = midIndex; i < this.getKeyCount(); ++i) {
			newRNode.setKey(i - midIndex, this.getKey(i));

			List<TValue> valueList = this.getValue(i);
			for (TValue value : valueList) {
				newRNode.setValue(i - midIndex, value);
			}

			this.setKey(i, null);
			this.values[i] = new ArrayList<>();
		}

		newRNode.keyCount = this.getKeyCount() - midIndex;
		this.keyCount = midIndex;

		newRNode.setNext(this.getNext());
		this.setNext(newRNode);

		return newRNode;
	}


	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
		throw new UnsupportedOperationException();
	}

	/* The codes below are used to support deletion operation */

	public boolean delete(TKey key) {
		int index = this.search(key);
		if (index == -1)
			return false;

		this.deleteAt(index);
		return true;
	}

	public boolean delete(TKey key, TValue value) {
		int index = this.search(key);
		if (index == -1) return false;

		List<TValue> valueList = this.values[index];
		valueList.remove(value);
		if (valueList.isEmpty()) {
			this.deleteAt(index);
		}
		return true;
	}


	private void deleteAt(int index) {
		for (int i = index; i < this.getKeyCount() - 1; ++i) {
			this.setKey(i, this.getKey(i + 1));
			this.values[i] = this.values[i + 1];
		}

		this.setKey(this.getKeyCount() - 1, null);
		this.values[this.getKeyCount() - 1] = new ArrayList<>();
		--this.keyCount;
	}


	@Override
	protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Notice that the key sunk from parent is be abandoned.
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) {
		BTreeLeafNode<TKey, TValue> siblingLeaf = (BTreeLeafNode<TKey, TValue>) rightSibling;

		int j = this.getKeyCount();
		for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
			this.setKey(j + i, siblingLeaf.getKey(i));

			for (TValue value : siblingLeaf.getValue(i)) {
				this.setValue(j + i, value);
			}
		}
		this.keyCount += siblingLeaf.getKeyCount();

		this.setRightSibling(siblingLeaf.rightSibling);
		if (siblingLeaf.rightSibling != null)
			siblingLeaf.rightSibling.setLeftSibling(this);

		this.setNext(siblingLeaf.getNext());
	}


	@Override
	@SuppressWarnings("unchecked")
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
		BTreeLeafNode<TKey, TValue> siblingNode = (BTreeLeafNode<TKey, TValue>) sibling;

		for (TValue value : siblingNode.getValue(borrowIndex)) {
			this.insertKey(siblingNode.getKey(borrowIndex), value);
		}
		siblingNode.deleteAt(borrowIndex);

		return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
	}


	@Override
	public String commit() {
		String result = "";
		for (int i = 0; i < values.length; i++) {
			if (values[i] == null) {
				break;
			}
			result += values[i].toString() + "\r\n";
		}
		return result;
	}

	@Override
	public BTreeLeafNode getSmallest() {
		return this;
	}

	// @Override
	// public String project(SelectColumns columns) {
	// String out = "";
	// for (int index = 0; index < this.getKeyCount(); ++index) {
	// if (filters.size() != 0) {
	// if (!filters.get(index)) {
	// continue;
	// }
	// }
	// TValue value = this.getValue(index);
	// if (value instanceof DBRecord) {
	// DBRecord record = (DBRecord) value;
	// String inc = record.project(columns);
	// out += inc + "\n";
	// } else {
	// out += this.getValue(index).toString() + " ";
	// }
	// }
	// filters = new ArrayList<>();
	// return out;
	// }

	// @Override
	// public void filter(ArrayList<DBCond> conditions) {
	// for (int index = 0; index < this.getKeyCount(); ++index) {
	// TValue value = this.getValue(index);
	// if (value instanceof DBRecord) {
	// DBRecord record = (DBRecord) value;
	// String inc = record.evaluate(conditions);
	// if (inc.equals("")) {
	// filters.add(false);
	// } else {
	// filters.add(true);
	// }
	// }
	// }
	// }
}