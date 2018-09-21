def swap(data, i, j):
    if i != j:
        data[i], data[j] = data[j], data[i]
    return data


class ListSort(object):
    # Time: O(n log(n)) average, best, O(n^2) worst
    # Space: O(n)
    @staticmethod
    def quick_sort(data):
        if data is None:
            raise TypeError('data cannot be None')
        if len(data) < 2:
            return data
        equal = []
        left = []
        right = []
        pivot_index = len(data) // 2
        pivot_value = data[pivot_index]
        # Build the left and right partitions
        for item in data:
            if item == pivot_value:
                equal.append(item)
            elif item < pivot_value:
                left.append(item)
            else:
                right.append(item)
        # Recursively apply quick_sort
        left_ = ListSort.quick_sort(left)
        right_ = ListSort.quick_sort(right)
        return left_ + equal + right_

    # Time: O(n^2)average, worst, best
    @staticmethod
    def selection_sort(data):
        if data is None:
            raise TypeError('data cannot be None')
        if len(data) < 2:
            return data
        for i in range(len(data) - 1):
            min_index = i
            for j in range(i + 1, len(data)):
                if data[j] < data[min_index]:
                    min_index = j
            if data[min_index] < data[i]:
                data[i], data[min_index] = data[min_index], data[i]
        return data

    # Time: O(n^2)average, worst, best
    # Space: O(m) recursive where m is the recursion depth
    @staticmethod
    def sort_recursive(data, start=0):
        if data is None:
            raise TypeError('data cannot be None')
        if len(data) < 2:
            return data
        if start < len(data) - 1:
            min_index = start
            for i in range(start + 1, len(data)):
                if data[i] < data[min_index]:
                    min_index = i
            swap(data, start, min_index)
            ListSort.sort_recursive(data, start + 1)
        return data

    # Time: O(n^2) average, worst, best
    # Space: O(1) iterative,
    @staticmethod
    def sort_iterative_alt(data, start=0):
        if data is None:
            raise TypeError('data cannot be None')
        if len(data) < 2:
            return data
        for i in range(len(data) - 1):
            min_index = i
            for j in range(i + 1, len(data)):
                if data[j] < data[min_index]:
                    min_index = j
            swap(data, i, min_index)
        return data

    # Time: O(n log(n))
    # Space: O(n)
    @staticmethod
    def merge_sort(data):
        if len(data) < 2:
            return data
        mid = len(data) // 2
        left = data[:mid]
        right = data[mid:]
        left = ListSort.merge_sort(left)
        right = ListSort.merge_sort(right)
        le = 0
        r = 0
        result = []
        while le < len(left) and r < len(right):
            if left[le] < right[r]:
                result.append(left[le])
                le += 1
            else:
                result.append(right[r])
                r += 1
        # Copy remaining elements
        while le < len(left):
            result.append(left[le])
            le += 1
        while r < len(right):
            result.append(right[r])
            r += 1
        return result

    # Time: O(k*n), where n is the number of items and k is the number of digits in the largest item
    # Space: O(k+n)
    @staticmethod
    def radix_sort(array, base=10):
        if array is None:
            raise TypeError('array cannot be None')
        if not array:
            return []
        max_element = max(array)
        max_digits = len(str(abs(max_element)))
        curr_array = array
        for digit in range(max_digits):
            buckets = [[] for _ in range(base)]
            for item in curr_array:
                print(item // (base ** digit) % base)
                buckets[(item // (base ** digit)) % base].append(item)
            curr_array = []
            for bucket in buckets:
                curr_array.extend(bucket)
        return curr_array

    @staticmethod
    def object_list_sort(object_list, compare_func=None):
        for i in range(len(object_list)):
            for j in range(i + 1, len(object_list), 1):
                cursor = compare_func(object_list[i], object_list[j])
                if cursor:
                    tmp = object_list[j]
                    object_list[j] = object_list[i]
                    object_list[i] = tmp
                else:
                    pass
