import bisect
from math import floor, ceil

class _StatsProperty(object):
    def __init__(self, name, func):
        self.name = name
        self.func = func
        self.internal_name = '_' + name

        doc = func.__doc__ or ''
        pre_doctest_doc, _, _ = doc.partition('>>>')
        self.__doc__ = pre_doctest_doc

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if not obj.data:
            return obj.default
        try:
            return getattr(obj, self.internal_name)
        except AttributeError:
            setattr(obj, self.internal_name, self.func(obj))
            return getattr(obj, self.internal_name)


class Stats(object):
    """The ``Stats`` type is used to represent a group of unordered
    statistical datapoints for calculations such as mean, median, and
    variance.
    Args:
        data (list): List or other iterable containing numeric values.
        default (float): A value to be returned when a given
            statistical measure is not defined. 0.0 by default, but
            ``float('nan')`` is appropriate for stricter applications.
        use_copy (bool): By default Stats objects copy the initial
            data into a new list to avoid issues with
            modifications. Pass ``False`` to disable this behavior.
        is_sorted (bool): Presorted data can skip an extra sorting
            step for a little speed boost. Defaults to False.
    """
    def __init__(self, data, default=0.0, use_copy=True, is_sorted=False):
        self._use_copy = use_copy
        self._is_sorted = is_sorted
        if use_copy:
            self.data = list(data)
        else:
            self.data = data

        self.default = default
        cls = self.__class__
        self._prop_attr_names = [a for a in dir(self)
                                 if isinstance(getattr(cls, a, None),
                                               _StatsProperty)]
        self._pearson_precision = 0

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def _get_sorted_data(self):
        """When using a copy of the data, it's better to have that copy be
        sorted, but we do it lazily using this method, in case no
        sorted measures are used. I.e., if median is never called,
        sorting would be a waste.
        When not using a copy, it's presumed that all optimizations
        are on the user.
        """
        if not self._use_copy:
            return sorted(self.data)
        elif not self._is_sorted:
            self.data.sort()
        return self.data

    def clear_cache(self):
        """``Stats`` objects automatically cache intermediary calculations
        that can be reused. For instance, accessing the ``std_dev``
        attribute after the ``variance`` attribute will be
        significantly faster for medium-to-large datasets.
        If you modify the object by adding additional data points,
        call this function to have the cached statistics recomputed.
        """
        for attr_name in self._prop_attr_names:
            attr_name = getattr(self.__class__, attr_name).internal_name
            if not hasattr(self, attr_name):
                continue
            delattr(self, attr_name)
        return

    def _calc_count(self):
        """The number of items in this Stats object. Returns the same as
        :func:`len` on a Stats object, but provided for pandas terminology
        parallelism.
        >>> Stats(range(20)).count
        20
        """
        return len(self.data)
    count = _StatsProperty('count', _calc_count)

    def _calc_mean(self):
        """
        The arithmetic mean, or "average". Sum of the values divided by
        the number of values.
        >>> mean(range(20))
        9.5
        >>> mean(list(range(19)) + [949])  # 949 is an arbitrary outlier
        56.0
        """
        return sum(self.data, 0.0) / len(self.data)
    mean = _StatsProperty('mean', _calc_mean)

    def _calc_max(self):
        """
        The maximum value present in the data.
        >>> Stats([2, 1, 3]).max
        3
        """
        if self._is_sorted:
            return self.data[-1]
        return max(self.data)
    max = _StatsProperty('max', _calc_max)

    def _calc_min(self):
        """
        The minimum value present in the data.
        >>> Stats([2, 1, 3]).min
        1
        """
        if self._is_sorted:
            return self.data[0]
        return min(self.data)
    min = _StatsProperty('min', _calc_min)

    def _calc_median(self):
        """
        The median is either the middle value or the average of the two
        middle values of a sample. Compared to the mean, it's generally
        more resilient to the presence of outliers in the sample.
        >>> median([2, 1, 3])
        2
        >>> median(range(97))
        48
        >>> median(list(range(96)) + [1066])  # 1066 is an arbitrary outlier
        48
        """
        return self._get_quantile(self._get_sorted_data(), 0.5)
    median = _StatsProperty('median', _calc_median)

    def _calc_iqr(self):
        """Inter-quartile range (IQR) is the difference between the 75th
        percentile and 25th percentile. IQR is a robust measure of
        dispersion, like standard deviation, but safer to compare
        between datasets, as it is less influenced by outliers.
        >>> iqr([1, 2, 3, 4, 5])
        2
        >>> iqr(range(1001))
        500
        """
        return self.get_quantile(0.75) - self.get_quantile(0.25)
    iqr = _StatsProperty('iqr', _calc_iqr)

    def _calc_trimean(self):
        """The trimean is a robust measure of central tendency, like the
        median, that takes the weighted average of the median and the
        upper and lower quartiles.
        >>> trimean([2, 1, 3])
        2.0
        >>> trimean(range(97))
        48.0
        >>> trimean(list(range(96)) + [1066])  # 1066 is an arbitrary outlier
        48.0
        """
        sorted_data = self._get_sorted_data()
        gq = lambda q: self._get_quantile(sorted_data, q)
        return (gq(0.25) + (2 * gq(0.5)) + gq(0.75)) / 4.0
    trimean = _StatsProperty('trimean', _calc_trimean)

    def _calc_variance(self):
        """\
        Variance is the average of the squares of the difference between
        each value and the mean.
        >>> variance(range(97))
        784.0
        """
        return mean(self._get_pow_diffs(2))
    variance = _StatsProperty('variance', _calc_variance)

    def _calc_std_dev(self):
        """\
        Standard deviation. Square root of the variance.
        >>> std_dev(range(97))
        28.0
        """
        return self.variance ** 0.5
    std_dev = _StatsProperty('std_dev', _calc_std_dev)

    def _calc_median_abs_dev(self):
        """\
        Median Absolute Deviation is a robust measure of statistical
        dispersion: http://en.wikipedia.org/wiki/Median_absolute_deviation
        >>> median_abs_dev(range(97))
        24.0
        """
        sorted_vals = sorted(self.data)
        x = float(median(sorted_vals))  # programmatically defined below
        return median([abs(x - v) for v in sorted_vals])
    median_abs_dev = _StatsProperty('median_abs_dev', _calc_median_abs_dev)
    mad = median_abs_dev  # convenience

    def _calc_rel_std_dev(self):
        """\
        Standard deviation divided by the absolute value of the average.
        http://en.wikipedia.org/wiki/Relative_standard_deviation
        >>> print('%1.3f' % rel_std_dev(range(97)))
        0.583
        """
        abs_mean = abs(self.mean)
        if abs_mean:
            return self.std_dev / abs_mean
        else:
            return self.default
    rel_std_dev = _StatsProperty('rel_std_dev', _calc_rel_std_dev)

    def _calc_skewness(self):
        """\
        Indicates the asymmetry of a curve. Positive values mean the bulk
        of the values are on the left side of the average and vice versa.
        http://en.wikipedia.org/wiki/Skewness
        See the module docstring for more about statistical moments.
        >>> skewness(range(97))  # symmetrical around 48.0
        0.0
        >>> left_skewed = skewness(list(range(97)) + list(range(10)))
        >>> right_skewed = skewness(list(range(97)) + list(range(87, 97)))
        >>> round(left_skewed, 3), round(right_skewed, 3)
        (0.114, -0.114)
        """
        data, s_dev = self.data, self.std_dev
        if len(data) > 1 and s_dev > 0:
            return (sum(self._get_pow_diffs(3)) /
                    float((len(data) - 1) * (s_dev ** 3)))
        else:
            return self.default
    skewness = _StatsProperty('skewness', _calc_skewness)

    def _calc_kurtosis(self):
        """\
        Indicates how much data is in the tails of the distribution. The
        result is always positive, with the normal "bell-curve"
        distribution having a kurtosis of 3.
        http://en.wikipedia.org/wiki/Kurtosis
        See the module docstring for more about statistical moments.
        >>> kurtosis(range(9))
        1.99125
        With a kurtosis of 1.99125, [0, 1, 2, 3, 4, 5, 6, 7, 8] is more
        centrally distributed than the normal curve.
        """
        data, s_dev = self.data, self.std_dev
        if len(data) > 1 and s_dev > 0:
            return (sum(self._get_pow_diffs(4)) /
                    float((len(data) - 1) * (s_dev ** 4)))
        else:
            return 0.0
    kurtosis = _StatsProperty('kurtosis', _calc_kurtosis)

    def _calc_pearson_type(self):
        precision = self._pearson_precision
        skewness = self.skewness
        kurtosis = self.kurtosis
        beta1 = skewness ** 2.0
        beta2 = kurtosis * 1.0

        # TODO: range checks?

        c0 = (4 * beta2) - (3 * beta1)
        c1 = skewness * (beta2 + 3)
        c2 = (2 * beta2) - (3 * beta1) - 6

        if round(c1, precision) == 0:
            if round(beta2, precision) == 3:
                return 0  # Normal
            else:
                if beta2 < 3:
                    return 2  # Symmetric Beta
                elif beta2 > 3:
                    return 7
        elif round(c2, precision) == 0:
            return 3  # Gamma
        else:
            k = c1 ** 2 / (4 * c0 * c2)
            if k < 0:
                return 1  # Beta
        raise RuntimeError('missed a spot')
    pearson_type = _StatsProperty('pearson_type', _calc_pearson_type)
    
    def _calc_mode(self):
      """\
      The mode is the value (or values) that occur the most number
      of times within the group. There is some dispute over whether
      an even distribution constitutes no mode, or they are all modes
      So, we have an attribute for each.
      	>>> s.mode
      	[]
      	>>> s.all_modes
      	[2.345, 3.456, 1.234, 9.012, 4.567, 7.89, 5.678]
      """
      listIn = self.data
      bins = list(set(self.data))
      counts = [self.data.count(x) for x in set(self.data)]
      maxCounts = max(counts)
      indices = [i for i, x in enumerate(counts) if x == maxCounts]
    
      dataOut = []
      if len(set(counts)) != 1:
        for i in indices:
          dataOut.append(bins[i])
      
      return dataOut
    mode = _StatsProperty('mode', _calc_mode)

    def _calc_all_modes(self):
      """\
      The mode is the value (or values) that occur the most number
       of times within the group. There is some dispute over whether
      an even distribution constitutes no mode, or they are all modes
      So, we have an attribute for each.
   	   >>> s.mode
   	   []
   	   >>> s.all_modes
   	   [2.345, 3.456, 1.234, 9.012, 4.567, 7.89, 5.678]
      """
      listIn = self.data
      bins = list(set(self.data))
      counts = [self.data.count(x) for x in set(self.data)]
      maxCounts = max(counts)
      indices = [i for i, x in enumerate(counts) if x == maxCounts]
    
      dataOut = []
      for i in indices:
        dataOut.append(bins[i])
      
      return dataOut
    all_modes = _StatsProperty('all_modes', _calc_all_modes)

    @staticmethod
    def _get_quantile(sorted_data, q):
        data, n = sorted_data, len(sorted_data)
        idx = q / 1.0 * (n - 1)
        idx_f, idx_c = int(floor(idx)), int(ceil(idx))
        if idx_f == idx_c:
            return data[idx_f]
        return (data[idx_f] * (idx_c - idx)) + (data[idx_c] * (idx - idx_f))

    def get_quantile(self, q):
        """Get a quantile from the dataset. Quantiles are floating point
        values between ``0.0`` and ``1.0``, with ``0.0`` representing
        the minimum value in the dataset and ``1.0`` representing the
        maximum. ``0.5`` represents the median:
        >>> Stats(range(100)).get_quantile(0.5)
        49.5
        """
        q = float(q)
        if not 0.0 <= q <= 1.0:
            raise ValueError('expected q between 0.0 and 1.0, not %r' % q)
        elif not self.data:
            return self.default
        return self._get_quantile(self._get_sorted_data(), q)

    def get_zscore(self, value):
        """Get the z-score for *value* in the group. If the standard deviation
        is 0, 0 inf or -inf will be returned to indicate whether the value is
        equal to, greater than or below the group's mean.
        """
        mean = self.mean
        if self.std_dev == 0:
            if value == mean:
                return 0
            if value > mean:
                return float('inf')
            if value < mean:
                return float('-inf')
        return (float(value) - mean) / self.std_dev

    def trim_relative(self, amount=0.15):
        """A utility function used to cut a proportion of values off each end
        of a list of values. This has the effect of limiting the
        effect of outliers.
        Args:
            amount (float): A value between 0.0 and 0.5 to trim off of
                each side of the data.
        .. note:
            This operation modifies the data in-place. It does not
            make or return a copy.
        """
        trim = float(amount)
        if not 0.0 <= trim < 0.5:
            raise ValueError('expected amount between 0.0 and 0.5, not %r'
                             % trim)
        size = len(self.data)
        size_diff = int(size * trim)
        if size_diff == 0.0:
            return
        self.data = self._get_sorted_data()[size_diff:-size_diff]
        self.clear_cache()

    def _get_pow_diffs(self, power):
        """
        A utility function used for calculating statistical moments.
        """
        m = self.mean
        return [(v - m) ** power for v in self.data]

    def _get_bin_bounds(self, count=None, with_max=False):
        if not self.data:
            return [0.0]  # TODO: raise?

        data = self.data
        len_data, min_data, max_data = len(data), min(data), max(data)

        if len_data < 4:
            if not count:
                count = len_data
            dx = (max_data - min_data) / float(count)
            bins = [min_data + (dx * i) for i in range(count)]
        elif count is None:
            # freedman algorithm for fixed-width bin selection
            q25, q75 = self.get_quantile(0.25), self.get_quantile(0.75)
            dx = 2 * (q75 - q25) / (len_data ** (1 / 3.0))
            bin_count = max(1, int(ceil((max_data - min_data) / dx)))
            bins = [min_data + (dx * i) for i in range(bin_count + 1)]
            bins = [b for b in bins if b < max_data]
        else:
            dx = (max_data - min_data) / float(count)
            bins = [min_data + (dx * i) for i in range(count)]

        if with_max:
            bins.append(float(max_data))

        return bins

    def histogram(self, bins=None, raw = None, **kw):
        """Produces a list of ``(bin, count)`` pairs comprising a histogram of
        the Stats object's data, using fixed-width bins. See
        :meth:`Stats.histogram` for more details.
        Args:
            bins (int): maximum number of bins, or list of
                floating-point bin boundaries. Defaults to the output of
                Freedman's algorithm.
            bin_digits (int): Number of digits used to round down the
                bin boundaries. Defaults to 1.
        """
        bin_digits = int(kw.pop('bin_digits', 1))
        if kw:
            raise TypeError('unexpected keyword arguments: %r' % kw.keys())

        if not bins:
            bins = self._get_bin_bounds()
        else:
            try:
                bin_count = int(bins)
            except TypeError:
                try:
                    bins = [float(x) for x in bins]
                except Exception:
                    raise ValueError('bins expected integer bin count or list of float bin boundaries, not %r' % bins)
                if self.min < bins[0]:
                    bins = [self.min] + bins
            else:
                bins = self._get_bin_bounds(bin_count)

        # floor and ceil really should have taken ndigits, like round()
        round_factor = 10.0 ** bin_digits
        bins = [floor(b * round_factor) / round_factor for b in bins]
        bins = sorted(set(bins))

        idxs = [bisect.bisect(bins, d) - 1 for d in self.data]
        count_map = {}  # would have used Counter, but py26 support
        for idx in idxs:
            try:
                count_map[idx] += 1
            except KeyError:
                count_map[idx] = 1

        bin_counts = [(b, count_map.get(i, 0)) for i, b in enumerate(bins)]
        
        if raw:
          h=[]
          for i in bin_counts:
            h.extend([i[0]]*i[1])
          bin_counts = h

        return bin_counts

    def describe(self, quantiles=None, format=None):
        """Provides standard summary statistics for the data in the Stats
        object, in one of several convenient formats.
        Args:
            quantiles (list): A list of numeric values to use as
                quantiles in the resulting summary. All values must be
                0.0-1.0, with 0.5 representing the median. Defaults to
                ``[0.25, 0.5, 0.75]``, representing the standard
                quartiles.
            format (str): Controls the return type of the function,
                with one of three valid values: ``"dict"`` gives back
                a :class:`dict` with the appropriate keys and
                values. ``"list"`` is a list of key-value pairs in an
                order suitable to pass to an OrderedDict or HTML
                table. ``"text"`` converts the values to text suitable
                for printing, as seen below.
        Here is the information returned by a default ``describe``, as
        presented in the ``"text"`` format:
        >>> stats = Stats(range(1, 8))
        >>> print(stats.describe(format='text'))
        count:    7
        mean:     4.0
        std_dev:  2.0
        mad:      2.0
        min:      1
        0.25:     2.5
        0.5:      4
        0.75:     5.5
        max:      7
        For more advanced descriptive statistics, check out my blog
        post on the topic `Statistics for Software
        <https://www.paypal-engineering.com/2016/04/11/statistics-for-software/>`_.
        """
        if format is None:
            format = 'dict'
        elif format not in ('dict', 'list', 'text'):
            raise ValueError('invalid format for describe,'
                             ' expected one of "dict"/"list"/"text", not %r'
                             % format)
        quantiles = quantiles or [0.25, 0.5, 0.75]
        q_items = []
        for q in quantiles:
            q_val = self.get_quantile(q)
            q_items.append((str(q), q_val))

        items = [('count', self.count),
                 ('mean', self.mean),
                 ('std_dev', self.std_dev),
                 ('mode', self.mode),
                 ('kurtosis', self.kurtosis),
                 ('skewness', self.skewness),
                 ('mad', self.mad),
                 ('min', self.min)]

        items.extend(q_items)
        items.append(('max', self.max))
        if format == 'dict':
            ret = dict(items)
        elif format == 'list':
            ret = items
        elif format == 'text':
            ret = '\n'.join(['%s%s' % ((label + ':').ljust(10), val)
                             for label, val in items])
        return ret


def buildSpcData(inputData, ignitionTagPath, spcFilter):
	"""
	Function used to build histogram chart data
	
	Parameters
	----------
	inputData: list
		list of data
		
	Returns
	-------
	dataset
		data prepared for chart
	"""	
	
	if inputData is None:
		return None 
			
	from com.inductiveautomation.ignition.common import TypeUtilities
	
	inputData = filter(lambda value: value != -99.0, inputData)
	# load statistics library
	statistics = Stats(inputData)
	# calculate data
	meanValue = system.math.mean(inputData)
	minValue = system.math.min(inputData)
	maxValue = system.math.max(inputData)
	standardDev = system.math.standardDeviation(inputData)
	
	# TO DO - DYNAMICALLY BRING LIMITS AND SETPOINT
	category = spcFilter["filter"]["category"]
	eqPath = system.tag.read(ignitionTagPath + "/mes/param_mesObject").value 
	currentMaterial = system.tag.read(ignitionTagPath + "/mes/prod_productCode").value
	cpList = shared.mes.referenceValues.getDataFromOperationDefinition(eqPath, currentMaterial)
	
	# Set default values
	hsp = lsp = sp = None
	bins = 20

	if "TW_histBins" in cpList:
		bins = int(cpList["TW_histBins"])
	
	if category == "weight":
		if len(cpList) > 0:
			for cp in cpList:
				if cp == "TW_weightLSP":
					lsp = float(cpList["TW_weightLSP"])
				if cp == "TW_weightHSP":
					hsp = float(cpList["TW_weightHSP"])
				if cp == "TW_weightTarget":
					sp = float(cpList["TW_weightTarget"])
	
	elif category == "thickness":
		if len(cpList) > 0:
			for cp in cpList:
				if cp == "TW_thickLSP":
					
					TW_thickLSP = str(cpList["TW_thickLSP"])
					TW_thickLSP = TW_thickLSP.replace(",",".")
					
					try:
						lsp = float(TW_thickLSP)
					except:
						lsp = 0.0

				if cp == "TW_thickHSP":
					
					TW_thickHSP = str(cpList["TW_thickHSP"])
					TW_thickHSP = TW_thickHSP.replace(",",".")
					
					try:
						hsp = float(TW_thickHSP)
					except:
						hsp = 0.0
					
				if cp == "TW_thickTarget":

					
					TW_thickTarget = str(cpList["TW_thickTarget"])
					TW_thickTarget = TW_thickTarget.replace(",",".")
					
					try:
						sp = float(TW_thickTarget)
					except:
						sp = 0.0

	#hsp = 37.00
	#lsp = 27.00
	#sp = 31.00
	# We want category every 0.5
	#step = 0.5
	#bins = (maxValue-minValue)/step
	headers = ["Label", "Value"]
	data = []
	
	# setting default values
	cp = cpk = 0.00
	
	# standard deviation is needed to calculate cp and cpk values
	# if standard deviation > 0 build, otherwise return as none
	if standardDev > 0:
		histogram = statistics.histogram(bins=bins, bin_digits=3)
				
		# build data for histogram
		for dataPoint in histogram:
			value = dataPoint[0]
			samples = int(dataPoint[1])
			data.append([value, samples])
		
		if hsp > 0.0 and lsp > 0.0:
			cp = (hsp - lsp) / (6 * standardDev)
			cpk1 = (hsp - meanValue) / (3 * standardDev)
			cpk2 = (meanValue - lsp) / (3 * standardDev)
			
			cpkList = [cpk1, cpk2]
			cpk = system.math.min(cpkList)
		
		else:
			cpk = 0.0
			cp = 0.0
		
	dataDS = system.dataset.toDataSet(headers, data)
	
	# return everything
	data = {
		'histogram': str(TypeUtilities.datasetToJSON(dataDS)),
		'cp': round(cp, 2),
		'cpk': round(cpk, 2),
		'hsp': hsp,
		'lsp': lsp,
		'sp': sp,
		'maxValue': maxValue,
		'minValue': minValue
		#'nbBins': bins,
		#'step': step
	}
	
	return data
	
###################################################	
def getRawData(startDate, endDate, press, spcFilter, dateQueryString = ""):
	"""
	Function used to get raw data from database
	
	Parameters
	----------
	startDate: date
		start of data period
	endDate: date
		end of data period
	press: str
		press identifier (P55, P54, ...)
	spcFilter: json
		filtration of data (reflects what will be returned as result)
		
	Returns
	-------
	sqlResult: pyDataSet
		database result
	"""	
	
	category = spcFilter["filter"]["category"]
	molds = spcFilter["filter"]["molds"]
	
	if len(category) < 1:
		system.gui.warningBox("Please select CATEGORY in filter !")
		return None
	if len(molds) < 1:
		system.gui.warningBox("Please select at least 1 MOLD in filter !")
		return None
	
	moldsQuery = ",".join(map(str, molds))
	
	querySelection = ""
	if category == "weight":
		querySelection = """TRIM(BOTH '"' FROM CAST(weights AS CHAR CHARACTER SET utf8)) as weights"""
	if category == "thickness":
		querySelection = """TRIM(BOTH '"' FROM CAST(thickness AS CHAR CHARACTER SET utf8)) as thickness"""

	# dynamically build filter based on date periods
	filter = ""
	if len(dateQueryString) > 0:
		filter = " AND (" + dateQueryString + ")"

	sqlQuery = """
		SELECT
			""" + querySelection + """
		FROM
			cycles 
		WHERE
			cycleEndDate > ? AND 
			cycleEndDate < ? AND 
			press = ? AND 
			table1Mold IN (""" + moldsQuery + """)  
			""" + filter + """
	"""

	sqlResult = system.db.runPrepQuery(sqlQuery, [startDate, endDate, press], "sga_twpress")
	
	return sqlResult	


###################################################		
def prepareSpcData(data, spcFilter):
	"""
	Function that transfers database result to SPC data as list
	
	Parameters
	----------
	data: pyDataset
		raw data as it comes from db
	spcFilter: json
		filtration of data (reflects what will be returned as result)
		
	Returns
	-------
	spcData: list
		list of data for building histogram
	"""	
	if data is None:
		return None 
		
	category = spcFilter["filter"]["category"]
	molds = spcFilter["filter"]["molds"]
	cavities = spcFilter["filter"]["cavities"]
	
	spcData = []
	
	for row in data:
		rowData = system.util.jsonDecode((row[0]))

		if category == "weight":
			measurementData = rowData["w"]
		if category == "thickness":
			measurementData = rowData["t"]
		
		if len(measurementData) == len(cavities):
			for cavity in cavities:
				value = measurementData[cavity - 1]
				spcData.append(value)
	
	return spcData	

def getRejectsPerMould(ignitionTagPath, runPeriod = False):
	tagsToRead = [
		"[System]Gateway/CurrentDateTime",
		ignitionTagPath + "/mes/param_stationName",
		ignitionTagPath + "/press/param_nbMolds",
		ignitionTagPath + "/press/analysis/scopeProductionRun/param_startDate"
	]
	
	tagValues = system.tag.readAll(tagsToRead)
	
	if runPeriod:
		startDate = tagValues[3].value
	else:
		startDate = system.date.addHours(tagValues[0].value, -1)
		
	press = tagValues[1].value
	param_nbMolds = tagValues[2].value	
	
	# try to get data from cycles table
	sqlQuery = """
		SELECT 
			table1Mold as moldId, 
			SUM(rejects) as totalRejectsPerMold,
			(SUM(rejects) / SUM(outfeed)) * 100 as rejectsPerMoldPercentage
		FROM 
			cycles 
		WHERE 
			cycleEndDate > ? AND 
			press = ? AND 
			table1Mold > 0
		GROUP BY 
			table1Mold
	"""
	
	result =  system.db.runPrepQuery(sqlQuery, [startDate, press], "sga_twpress")
	
	# if no results, we manually build result as static
	if len(result) < 1:
		headers = ["moldId", "totalRejectsPerMold", "rejectsPerMoldPercentage"]
		data = []
		
		for mold in range(1, param_nbMolds + 1):
			data.append([
				mold, 
				0,
				0
			])
			
		result = system.dataset.toDataSet(headers, data)
		
	return result

	
			


def datasetToJson(dataset):
	# Browse rows
	jsonList = {"headers": dataset.getColumnNames(),   "values": [] }
	for rowIdx in range(dataset.rowCount):
		# Build lists of each line
		jsonList["values"].append( [dataset.getValueAt(rowIdx, colIdx) for colIdx in range(dataset.columnCount)])
	return jsonList

def getTriggerData(ignitionTagPath, journalName, startDate, endDate):
	
	def convertUTCToDateTime(timestampInput):
		import datetime
		
		datetimeValue = datetime.datetime.fromtimestamp(timestampInput).strftime('%Y-%m-%d %H:%M:%S')
		
		return datetimeValue
	
	#Function that extracts ackowledge data
	def getAckValues(trigger):
		ackNotes = ackUser = action = checkListName = ""
		checkListExec = 0
		
		for triggerProperty in trigger:
			triggerPropertyName = triggerProperty.getProperty()
			
			
			if str(triggerPropertyName) == "ackUser":
				ackUser = str(triggerProperty.getValue())
				ackUser = ackUser.split(":")[-1]
			
			elif str(triggerPropertyName) == "checklistName":
				checkListName = (triggerProperty.getValue()) 
			
			elif str(triggerPropertyName) == "ackNotes":
				ackNotes = str(triggerProperty.getValue())
			
			elif str(triggerPropertyName) == "action":
				action = str(triggerProperty.getValue())
				
				#0 = Not executed yet
				#1 = Executed
				#2 = Declined
				
				checkListExec = 0
				
				if "*EXECUTED*" in ackNotes:
					checkListExec = 1
				elif "*DECLINED*" in ackNotes:
					checkListExec = 2 
				
		return ackNotes, ackUser, checkListName, checkListExec, action
	
	ignitionTagPathSplit = ignitionTagPath.split("[default]")
	
	if len(ignitionTagPathSplit) > 1:
		source = "*"+ignitionTagPathSplit[1]+"*"
	else:
		source = "*"+ignitionTagPath+"*"

	
	
	#Get all history dataset
	alarms = system.alarm.queryJournal(
		journalName = journalName, 
		path = [source],
		startDate = startDate, 
		endDate = endDate,
		includeSystem = False,
		includeData = True
	)
	
	#Build History table header and row variable
	headers = [
		"Active Time",
		"Name",
		"Current State",
		"Priority",
		"Action",
		"Ack. Note",
		"Ack. User",
		"checkListName",
		"checkListExec"
	]
	
	data = []
	
	#Loop though dataset and fill row data
	for alarm in alarms:
		isCleared = alarm.isCleared()
		isAcked = alarm.isAcked()
		
		currentState = alarm.getState()
		
		ackNote = action = ackUser = checkListName = ""
		checkListExec = 0
		
		if isCleared:
			triggerData = alarm.getClearedData()
		elif isAcked:
			triggerData = alarm.getAckData()
		else:
			triggerData = alarm.getActiveData()
		
		triggerDataValues = triggerData.getValues()
		ackValues = getAckValues(triggerDataValues)	
		ackNote = ackValues[0]
		ackUser = ackValues[1]
		checkListName = ackValues[2]
		checkListExec = ackValues[3]
		action = 		ackValues[4]
	
		eventTime = triggerData.getTimestamp()
		eventTime = convertUTCToDateTime(eventTime / 1000)
		eventTime = system.date.parse(eventTime, "yyyy-MM-dd HH:mm:ss")
		eventTime = system.date.format(eventTime, "yyyy-MM-dd HH:mm:ss")
		
		name = alarm.getName()
		currentState = alarm.getState()
		priority = alarm.getPriority()
		label = alarm.getLabel()
		
		data.append([
			eventTime,
			name,
			currentState,
			priority,
			action,
			ackNote,
			ackUser,
			checkListName,
			checkListExec
		])
		
	dataDS = system.dataset.toDataSet(headers, data)
	dataDS = system.dataset.sort(dataDS, 0, False)
	
	return dataDS

def selectFeedbackStatus(feedbackId):
	"""
	Function to select status of feedbackSteps
	
	Parameters
	----------
	feedbackId: int
		internal id for feedback
	
	Returns
	-------
	Pydataset
		Dataset containing feedback status
	"""
	sql = """SELECT 
			(
			    CASE 
			        WHEN solved = 1 AND f3.stepType = 1 THEN "YES"
			        WHEN solved = -1 AND f3.stepType = 1 THEN "NO"
			        WHEN solved = 1 AND f3.stepType = 2 THEN "OK"
			        WHEN solved = 2 AND f3.stepType = 3 THEN "SOLVED"
			        WHEN solved = -1 AND f3.stepType = 3 THEN "NOT SOLVED"
			        WHEN solved = 1 AND f3.stepType = 4 THEN "CONFIRMED"
			        WHEN solved = 1 AND f3.stepType = 5 THEN "NEXT"
			        ELSE "NOT SOLVED"
			    END) AS status,
			f1.stepnumber stepNumber, f2.user, f1.startTime, f1.endTime, 
			(
				CASE
					WHEN f3.stepType = 1 THEN "Decision step"
					WHEN f3.stepType = 2 THEN "Check step"
					WHEN f3.stepType = 3 THEN "Default step"
					WHEN f3.stepType = 4 THEN "Confirmation step"
					WHEN f3.stepType = 5 THEN "Overview step"
				ELSE "UNKNOWN STEP TYPE"
				END) as stepTypeName,
				f3.issue as issue  
			FROM feedbackstep f1
			LEFT JOIN feedback f2 on f1.feedbackId = f2.feedbackId
			LEFT JOIN step f3 on f3.stepId = f1.stepId 
			RIGHT JOIN (SELECT stepNumber, min(startTime), max(stepFeedbackId) id FROM feedbackstep where feedbackid = ? group by stepNumber) f2 ON f1.stepFeedbackId = f2.id  
			Order by f1.stepnumber 
			"""
	
	return system.db.runPrepQuery(query=sql, args=[feedbackId], database="sga_checklist")

def selectAnalyticChecklistV3(checkListExecData, startDate, endDate):
	"""
	Function to select few analytic for checklists
	
	Parameters
	----------
	category: str
		category for filtering
	
	Returns
	-------
	Pydataset
		Dataset containing analytic table
	"""
	
	checkListData = {}
	feedbackSql = """
	SELECT feedback.feedbackId, feedback.comment as comment, feedback.duration / 60000 as duration
		FROM feedback 
		JOIN feedbackstep ON (feedback.feedbackId = feedbackstep.feedbackId) 
		WHERE feedback.checkListId = ? 
		AND startTimeStamp BETWEEN ? AND ?
		GROUP BY feedback.feedbackId
	"""
		
	stepCountSql = """select count(stepId) as stepsNumber from step where checkListId = ?"""
	
	
	for key in checkListExecData.keys():
		checkListData[key] = {} 
		checkListUUID = checkListExecData[key]["uuid"]
		# Loop over checklists and give me the analytics
		#for checklist in checklists:
		feedbackDs = system.db.runPrepQuery(query=feedbackSql, args=[checkListUUID,startDate, endDate], database="sga_checklist")
		feedbackDsPy = system.dataset.toPyDataSet(feedbackDs)
		
		sqlResult = system.db.runPrepQuery(query=stepCountSql, args=[checkListUUID], database="sga_checklist")
		stepCount = sqlResult[0]["stepsNumber"]
	
		
		for row in feedbackDsPy:
			status = selectFeedbackStatus(row["feedbackId"])
			checkListData[key][row["feedbackId"]] = {
				"totalSteps": stepCount, 
				"stepsProcessed": status.getRowCount(), 
				"skippedSteps": stepCount - status.getRowCount(),
				"operatorComment" : row["comment"],
				"durationTotalMin": row["duration"],
				"feedbackDetails": datasetToJson(status) 
				}
			
	
	return	checkListData	

def stateDurationAndCountTw(eqPath, startDate, endDate):
	
	"""
	Function to run an Analysis for state durtation and recurrence in given time period.
	
	Input:
		eqPath:
			string
				Equipment path
		startDate
			date
		endDate
			date
	Output:
		statedurationAndCount
			dict
	"""
	
	analysis_setting = system.mes.analysis.createMESAnalysisSettings("recurrenceAndDurationOfStatePerShift")
	datapoints = [
		"Line State Event Begin",
		"Line State Duration",
		"Equipment Original State Value",
		"Line State Name",
		"Line State Type"
	]
			
	analysis_setting.setDataPoints(datapoints)
	analysis_setting.addParameter('path')
	analysis_setting.setFilterExpression("Equipment Path = @path ")
	analysis_setting.setGroupBy("Line State Event Begin")
	analysis_setting.setOrderBy("Line State Duration")
	
	params = {'path':eqPath}
	
	
	analysisData = system.mes.analysis.executeAnalysis(startDate, endDate, analysis_setting, params).getDataset()
	
	
	analysisPy = system.dataset.toPyDataSet(analysisData)
	
	
	stateReoccurance = dict()

	
	for row in analysisPy:

		if row["Equipment Original State Value"] not in stateReoccurance:
			stateReoccurance[row["Equipment Original State Value"]] = {
				"stateName": 	row["Line State Name"], 
				"stateType": 	row["Line State Type"],
				"count": 		1,
				"duration":		row["Line State Duration"]
			}
		else:
			stateReoccurance[row["Equipment Original State Value"]]["count"] += 1
			stateReoccurance[row["Equipment Original State Value"]]["duration"] += row["Line State Duration"]
	
	return stateReoccurance


def getShiftTimes(eqPath, prodRunStartDate, prodRunEndDate, shifts = [], startDate = None):
	"""
	Function to build shift periods, based on selected shifts
	
	Parameters
	----------
	eqPath: string
		path to equipment line in production model
	shifts: list
		list of selected equipment shifts
		
	Returns
	-------
	data: list
		data including last 1h period
	"""		
	def parseSchedule(realSchedule):
		"""
		Helper function to parse schedule strings to array of values
		"""
		
		#Make sure that we have ':' everywhere - appearantly '.' is also allowed
		realSchedule = realSchedule.replace(".", ":")
		realScheduleSplit = None
		
		if "," in realSchedule:
			shiftTimes = []
			for schedule in realSchedule.split(","):
				shiftTimes.append(schedule.split("-"))
		else:
			shiftTimes = [realSchedule.split("-")]
								
		return shiftTimes
		
	shiftTimesData = []
	
	shiftCount = 1
	eq =  system.mes.getMESObjectLinkByEquipmentPath(eqPath)
	eqObj = eq.getMESObject()
	ignitionSchedule = eqObj.getIgnitionSchedule()
	schedules = ignitionSchedule.split(",")

	
	tmpDate = system.date.now() if not startDate else startDate
		
	
	nbDays = system.date.daysBetween(prodRunStartDate, prodRunEndDate)
	day = system.date.getDayOfWeek(tmpDate)
	
	for sh in schedules:
		realSchedule = system.user.getSchedule(sh)

		if realSchedule.isAllDays():
			shiftTimes = parseSchedule(realSchedule.getAllDayTime())
			days = [1,2,3,4,5,6,7]
		elif realSchedule.isWeekDays():
			shiftTimes = parseSchedule(realSchedule.getWeekDayTime())
			days = [2,3,4,5,6]
		else:
			days = []
			if day == 1:
				shiftTimes = parseSchedule(realSchedule.getSundayTime())
				days.append(1)
			elif day == 2:
				shiftTimes = parseSchedule(realSchedule.getMondayTime())
				days.append(2)
			elif day == 3:
				shiftTimes = parseSchedule(realSchedule.getTuesdayTime())
				days.append(3)
			elif day == 4:
				shiftTimes = parseSchedule(realSchedule.getWednesdayTime())
				days.append(4)
			elif day == 5:
				shiftTimes = parseSchedule(realSchedule.getThursdayTime())
				days.append(5)
			elif day == 6:
				shiftTimes = parseSchedule(realSchedule.getFridayTime())
				days.append(6)
			elif day == 7:
				shiftTimes = parseSchedule(realSchedule.getSaturdayTime())
				days.append(7)
		
		for tmp in shiftTimes:
			endDateHour = int(tmp[1].split(":")[0])
			endDateMin = int(tmp[1].split(":")[1])
			startDateHour = int(tmp[0].split(":")[0])
			startDateMin = int(tmp[0].split(":")[1])
			transition = False
			
			startDate = system.date.setTime(tmpDate,startDateHour,startDateMin,0)
			
			
			endDate = system.date.setTime(tmpDate,endDateHour,endDateMin,0)
			
			shiftName = realSchedule.getName()
			
			if system.date.isAfter(startDate,endDate):
				transition = True
				endDate = system.date.addDays(endDate,1)
			
			shiftTimesData.append([0,shiftName,startDate,endDate,transition, days])
			
		
	#shiftTimesData.sort(key=lambda x: x[1])
	
	count = 0
	prevShift = ""		
	for idx,r in enumerate(shiftTimesData):
		
		if prevShift <> shiftTimesData[idx][1]:
			count += 1 
		
		shiftTimesData[idx][0] = count
		shiftTimesData[idx][2] = system.date.format(shiftTimesData[idx][2], "HH:mm:ss")
		shiftTimesData[idx][3] = system.date.format(shiftTimesData[idx][3], "HH:mm:ss")			
		prevShift = shiftTimesData[idx][1]  
	
	#If we need to remove some shifts
	shiftCount = 0	
	for shift in shifts:			
		if not shift:
			del shiftTimesData[shiftCount]
			shiftCount -= 1
		shiftCount += 1
			
		
	return shiftTimesData
	
	
def getShiftsInProdRun(shiftTimesData, prodRunStartDate, prodRunEndDate):
	"""
	Function to format output of shiftTimes data to actual dates in prod run time frames
	"""
	
	#Get number of days fro number of loops
	nbDays = 	system.date.daysBetween(prodRunStartDate, prodRunEndDate)
	
	#Staring day is prod run start day
	day = system.date.getDayOfWeek(prodRunStartDate) 

	#Starting temp date is prodRunStartDate
	date = prodRunStartDate

	#Define variables
	newData = []
	count = 1
	 
	#Loop days
	for d in range(1, nbDays+2):
		#Loop shift times data
		for row in shiftTimesData:
			#Check if current day of week in possible weekDays
			if day in row[5]:
				#If yes then parse times with current temp date
				endDateHour = int(row[3].split(":")[0])
				endDateMin = int(row[3].split(":")[1])
				startDateHour = int(row[2].split(":")[0])
				startDateMin = int(row[2].split(":")[1])
				
				startDate = system.date.setTime(date,startDateHour,startDateMin,0)
				
				endDate = system.date.setTime(date,endDateHour,endDateMin,0)

				#This is skipped first iteration to find first shift
				#If already has data and is inside time frames
				if count > 1 and system.date.isAfter(startDate, prodRunStartDate) and system.date.isBefore(startDate, prodRunEndDate):
					
					#If it is in time frame
					if system.date.isBefore(endDate, prodRunEndDate):
						newData.append([count, row[0], row[1], startDate, endDate])
						count += 1
					
					#If it is last day and needs to exit
					if system.date.isBefore(prodRunEndDate, endDate):
						newData.append([count, row[0], row[1], startDate, prodRunEndDate])
						count += 1
				
				#This is to find first shift
				if count == 1:
					#If started in shift time
					if system.date.isAfter(date,startDate) and system.date.isBefore(date, endDate):
						newData.append([count, row[0], row[1], prodRunStartDate, endDate])
						count += 1
					
					#If not just try to compensate	
					else:
						newData.append([count, row[0], row[1], prodRunStartDate, system.date.addDays(startDate,1)])
						count += 1
					
				
		#Iterate date and day of week
		date = system.date.addDays(date,1)
		day = system.date.getDayOfWeek(date)
	
	return newData

def getDaysData(prodRunStartDate, prodRunEndDate):
	
	nbDays = system.date.daysBetween(prodRunStartDate, prodRunEndDate)
	
	startDay = 	system.date.getDayOfYear(prodRunStartDate)
	endDay =	system.date.getDayOfYear(prodRunEndDate)
	
	tempDate = prodRunStartDate
	
	if startDay == endDay:
		return False
	else:
		daysData = []

		for d in range (1, nbDays+2):
			if d == 1:
				daysData.append([d, tempDate, system.date.setTime(system.date.addDays(tempDate,1),0,0,0)])
			
			elif d == nbDays+1:
				daysData.append([d, system.date.setTime(tempDate,0,0,0), prodRunEndDate])	
		
			elif d > 1 and d <> nbDays+2:
				daysData.append([d, system.date.setTime(tempDate,0,0,0), system.date.setTime(system.date.addDays(tempDate,1),0,0,0)])	

			system.date.addDays(tempDate,1)
	
	return daysData

def buildDefualtSpcFilters(param_nbMolds, prod_nbCavities):
	
#	tagsToRead = [
#		ignitionTagPath + "/press/param_nbMolds",
#		ignitionTagPath + "/press/prod_nbCavities"
#	]
#	
#	tagValues = 		system.tag.readBlocking(tagsToRead)
#	
#	param_nbMolds = 	tagValues[0].value
#	prod_nbCavities = 	tagValues[1].value
	
	molds = 				[]
	cavities = 				[]	
	spcFilterThickness = 	{}
	spcFilterWeight = 		{}
	
	for mold in range(1, param_nbMolds+1): molds.append(mold)
	for cavity in range(1, prod_nbCavities+1): cavities.append(cavity)
	
	spcFilterThickness["filter"] = 	{"molds": molds, "cavities": cavities, "category":"thickness"}
	spcFilterWeight["filter"] = 	{"molds": molds, "cavities": cavities, "category":"weight"}
	
	return [spcFilterThickness, spcFilterWeight]  
		

def buildCPKandTargetsDict(startDate, endDate, press, param_nbMolds, prod_nbCavities, ignitionTagPath):	
	
	spcFilters = buildDefualtSpcFilters(param_nbMolds, prod_nbCavities)
	spcDataDict = {}
	
	for spcFilter in spcFilters:
		rawData = getRawData(startDate, endDate, press, spcFilter)
		spcData = prepareSpcData(rawData, spcFilter)
		spcData = buildSpcData(spcData, ignitionTagPath, spcFilter)
	
		#spcData["histogram"] = system.util.jsonDecode(spcData["histogram"])
		del spcData["histogram"]
		spcDataDict[spcFilter["filter"]["category"]] = spcData
	return spcDataDict

def buildRunStreamData(ignitionTagPath):
	"""
	Function to build stream data for production run.
	According to specification:
		Ticket: SDSGA-4459
	
	Input:
		igntionTagPath
			string
	Output:
		runStreamData
			dict
	"""
	####################
	#Get data from tags#
	####################
	
	tagsToRead = [
		ignitionTagPath + "/press/prod_workOrder",
		ignitionTagPath + "/press/analysis/scopeProductionRun/prod_goodWheels",
		ignitionTagPath + "/press/analysis/scopeProductionRun/prod_rejectedWheels",
		ignitionTagPath + "/press/analysis/scopeProductionRun/prod_rejectsReinjected",
		ignitionTagPath + "/press/analysis/scopeProductionRun/prod_rejectsRejected",
		ignitionTagPath	+ "/press/analysis/scopeProductionRun/param_startDate",
		ignitionTagPath	+ "/press/analysis/scopeProductionRun/param_endDate",
		ignitionTagPath + "/mes/param_mesObject",
		ignitionTagPath + "/press/param_nbMolds",
		ignitionTagPath + "/press/prod_nbCavities",
		ignitionTagPath + "/press/analysis/scopeProductionRun/param_stationName",
		"[default]Factory/param_alarm_journal"
	]
	
	tagValues = system.tag.readBlocking(tagsToRead)
	
	prod_workOrder = 		tagValues[0].value
	prod_goodWheels = 		tagValues[1].value
	prod_rejectedWheels = 	tagValues[2].value
	prod_rejectsReinjected =tagValues[3].value
	prod_rejectsRejected = 	tagValues[4].value
	prodRunStartDate = 		tagValues[5].value
	prodRunEndDate = 		tagValues[6].value
	eqPath = 				tagValues[7].value
	param_nbMolds =			tagValues[8].value
	prod_nbCavities =		tagValues[9].value
	press = 				tagValues[10].value
	param_alarm_journal = 	tagValues[11].value
	###################
	#Rejects per mould#
	###################
	
	rejectPerMoldDS = getRejectsPerMould(ignitionTagPath, runPeriod = True)
	
	rejectPerMoldDSPy = system.dataset.toPyDataSet(rejectPerMoldDS)
	rejectPerMoldDict = {}
	
	for row in rejectPerMoldDSPy:
		rejectPerMoldDict[row["moldId"]] = {
			"totalRejectsPerMold": 		row["totalRejectsPerMold"],
			"rejectsPerMoldPercentage": row["rejectsPerMoldPercentage"]
		}
		
	#####################################
	#Duration of MES states + occurences#
	#####################################
	stateDurationAndCount = stateDurationAndCountTw(eqPath, prodRunStartDate, prodRunEndDate)
	
	
	##############
	#Trigger data#
	##############

	triggerData = getTriggerData(ignitionTagPath, param_alarm_journal, prodRunStartDate, prodRunEndDate)
	triggerDataPy = system.dataset.toPyDataSet(triggerData)
	
	triggerDataDict = 		{}
	checklistExecData = 	{}
	
	# Browse rows
	triggerDataDict = {"headers": triggerData.getColumnNames(),   "values": [] }
	sqlQueryChecklist = "SELECT uuid FROM checklist WHERE code = ?"
	for rowIdx in range(triggerData.getRowCount()):
		# Build lists of each line

		if triggerData.getValueAt(rowIdx, "checkListExec") > 0:
			if triggerData.getValueAt(rowIdx, "checkListName") not in checklistExecData:
				checklistExecData[triggerData.getValueAt(rowIdx, "checkListName")] = {
					"execCount" : 	0,
					"declineCount": 0,
					"uuid":	system.db.runScalarPrepQuery(sqlQueryChecklist, [triggerData.getValueAt(rowIdx, "checkListName")], "sga_checklist")			
				}
			
			if triggerData.getValueAt(rowIdx, "checkListName") in checklistExecData:
				if triggerData.getValueAt(rowIdx, "checkListExec") == 1:
					checklistExecData[triggerData.getValueAt(rowIdx, "checkListName")]["execCount"] 	+= 1
				elif triggerData.getValueAt(rowIdx, "checkListExec") == 2:
					checklistExecData[triggerData.getValueAt(rowIdx, "checkListName")]["declineCount"] += 1
		
		# Build lists of each line
		triggerDataDict["values"].append( [triggerData.getValueAt(rowIdx, colIdx) for colIdx in range(triggerData.getColumnCount())])
	
	
	################
	#Checklist data#
	################
	
	checkListData = selectAnalyticChecklistV3(checklistExecData, prodRunStartDate, prodRunEndDate)
	####################################
	#Custom proeprty data - SCP targets#  
	####################################
	spcData = buildCPKandTargetsDict(prodRunStartDate, prodRunEndDate, press, param_nbMolds, prod_nbCavities, ignitionTagPath)
	
	
	####################
	#TAKEN OUT OF SCOPE#
	####################
#	############
#	#Shift data#
#	############
#	
#	shiftTimesData = 	getShiftTimes(eqPath, prodRunStartDate, prodRunEndDate, startDate = prodRunStartDate)
#	shiftTimesInRun = 	getShiftsInProdRun(shiftTimesData, prodRunStartDate, prodRunEndDate)
#	
#	shiftData = {}
#	for row in shiftTimesInRun:
#		try:
#			oee = shared.mes.analysis.oee.getOEEData(row[3], row[4], eqPath)
#		except:
#			oee = 0
#		shiftData[row[0]] = {
#			"shiftNb" : 	row[1],
#			"shiftName": 	row[2],
#			"startDate":	row[3],
#			"endDate":		row[4],
#			"shiftOee":		oee
#		}
#
#	##########
#	#DailyOEE#
#	##########
#	
#	oeeData = {
#		"totalOee": shared.mes.analysis.oee.getOEEData(prodRunStartDate, prodRunEndDate, eqPath)
#	}
#	
#	daysData = getDaysData(prodRunStartDate, prodRunEndDate)
#	
#	if daysData:
#		for day in daysData:
#			oeeData["day"+str(day[0])] = {
#				"startDate": 	day[1],
#				"endDate": 		day[2],
#				"oee":			shared.mes.analysis.oee.getOEEData(day[1], day[2], eqPath)
#			}
#	
#	
#	
	
	##############
	#Build JSON###
	##############
	
	additionalJson = {}
	
	prod_goodWheels = 		tagValues[1].value
	prod_rejectedWheels = 	tagValues[2].value
	prod_rejectsReinjected =tagValues[3].value
	prod_rejectsRejected = 	tagValues[4].value
	
	additionalJson["workOrder"] = 			prod_workOrder
	additionalJson["rejectedWheels"] = 		prod_rejectedWheels
	additionalJson["rejectsReinjected"] = 	prod_rejectsReinjected
	additionalJson["rejectsRejected"] = 	prod_rejectsRejected
	additionalJson["goodWheels"] = 			prod_goodWheels
	additionalJson["press"] = 				press
	
	additionalJson["rejectPerMold"] =		rejectPerMoldDict
	additionalJson["stateDurationAndCount"]=stateDurationAndCount
	additionalJson["spcData"] = 			spcData
	
	#Not needed - Comment by Ivan and Pascal 02-11-2021
	#
	#additionalJson["shiftData"] = 			shiftData
	#additionalJson["daysData"] = 			daysData
	#additionalJson["oeeData"] =			oeeData
	
	additionalJson["triggerData"] =			triggerDataDict
	
	additionalJson["checkListExecData"] =	checklistExecData
	additionalJson["checkListData"] =		checkListData
	
	additionalJson["rejects"] =				shared.sga.tw.rejection.formatTWRejectJson(ignitionTagPath)		
	
	return additionalJson

def buildCycleStreamData(ignitionTagPath):
	"""
	Function to build the JSON for each cycle for TW Press.
	In general we are just reading the cycle tags of previous cycle and compiling to JSON format.
	
	Input:
		ignitionTagPath
	Output
		cycleDict
			dictionary
	"""
	
	def flexovitWeightAndThickness(ignitionTagPath):

			tagsToRead = [
				ignitionTagPath + "/signals/weights/weight1",
				ignitionTagPath + "/signals/thickness/thickness1",
				ignitionTagPath + "/signals/thickness/thickness2",
				ignitionTagPath + "/signals/thickness/thickness3"
			]

			tagObj = system.tag.readBlocking(tagsToRead)
			tagValues = []
			weightsJson = {}
			
			weightsJson["w"] = [tagObj[0].value]
		
			tagValues = [tagObj[1].value, tagObj[2].value, tagObj[3].value]
			thicknessJson = {}
			
			thicknessJson["t"] = tagValues

			#weightsJson = system.util.jsonEncode(weightsJson)
			#thicknessJson = system.util.jsonEncode(thicknessJson)
			
			return weightsJson, thicknessJson 
	
	def getCycleTimesDictPerStationDefinition(stationDefinitions, ignitionTagPath):
	
		"""
		Function that gets all station cycle times and compiles to show only the station times that are included
		in calculation and are named based on definitions
		
		Parameters
		----------
		ignitionTagPath : tagPath
			path to equipment in tag path structure
		
		Returns
		-------
		list
			slowestStationName: str
				name of the slowest station from press station definitions
			maxCycleTime: float
				slowest cycle time duration
		"""	
		
		parentPath = ignitionTagPath + "/signals/cycles/stations"
		stationCycleTimes = shared.sga.tw.stations.readStationCycleValues(ignitionTagPath)
		
		count = 0
		
		cycleTimeDict = {}
		
		for station in stationCycleTimes:
			# NOTE: name standard needs to be respected!!!
			# Like: prod_t01st05
			# getting table id, station id and actual cycle duration per station
			stationPartsData = station[0].split("_")[1]
			tableId = int(stationPartsData[2:3])
			stationId = int(stationPartsData[-2:])
			stationCycleTime = station[1]
			
			# loop through station definitions
			for station in system.dataset.toPyDataSet(stationDefinitions):
				if tableId == station["table"] and stationId == station["station"] and station["includeInCalculations"]:
					stationName = station["stationName"]
					
					cycleTimeDict[stationName] = stationCycleTime
	
				count += 1	
	
		return cycleTimeDict
	
	#Must be special casae for Flexovit
	#We identify by robotTable tag
	flexovitCase = system.tag.exists(ignitionTagPath + "/signals/molds/robotTable1Mold")
	
	tagsToRead = [
		ignitionTagPath + "/press/cycles/previous/db/cycleDuration",
		ignitionTagPath + "/press/cycles/previous/db/thickness",
		ignitionTagPath + "/press/cycles/previous/db/weights",
		ignitionTagPath + "/press/param_stationDefinitions",
		"[MES]"+ignitionTagPath.split("[default]")[1]+"/Shift/Current Shift"
	]

	
	
	
	tagValues = system.tag.readBlocking(tagsToRead)
	
	
	cycleDuration = 		tagValues[0].value
	if flexovitCase:
		weights, thickness = flexovitWeightAndThickness(ignitionTagPath)
	
	else:
		thickness = 			system.util.jsonDecode(tagValues[1].value)
		weights = 				system.util.jsonDecode(tagValues[2].value)
	
	stationDefinitions = 	tagValues[3].value
	currentShift = 			tagValues[4].value
	
	cycleTimes = getCycleTimesDictPerStationDefinition(stationDefinitions, ignitionTagPath)
	
	#############################
	#Missing CP, CPK and Targets#
	#############################
	#Taken out of scope
	#Maybe when we implement another logic to calculate
	
	
	additionalJson = {}
	
	additionalJson["cycleDuration"] = 		cycleDuration
	additionalJson["thickness"] = 			thickness
	additionalJson["weights"] = 			weights
	additionalJson["stationCycleTimes"] = 	cycleTimes
	additionalJson["currentShift"] = 		currentShift
	
	return additionalJson	
	