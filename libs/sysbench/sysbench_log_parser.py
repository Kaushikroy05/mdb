import re

class SysbenchParseLogfile:

    SECTIONS = [
        'SQL statistics',
        'General statistics',
        'Latency (ms)',
        'Threads fairness',
    ]

    def __new__(cls, filepath):
        return cls._get_sysbench_stats(filepath)

    @classmethod
    def _get_sysbench_stats(cls, filepath):

        def _modifystr(st):
            """
            given a string:
                1. remove any substring after '('
                2. strip, and
                3. replace spaces with underscore
            """
            st = st.split('(')[0]
            st = st.strip()
            st = st.replace(' ', '_')
            return st

        stats = cls._parse_log(filepath)
        stats_dict = dict()

        for section_name in cls.SECTIONS:
            section_dict = stats[section_name]
            section_name_us = _modifystr(section_name)
            stats_dict[section_name_us] = dict()

            for section_stat in section_dict:
                stat_split = section_stat.split(':')
                if len(stat_split) != 2:
                    continue
                else:
                    stat_attr_name = _modifystr(stat_split[0])
                    stat_attr_value = _modifystr(stat_split[1])
                    stats_dict[section_name_us][stat_attr_name] = stat_attr_value

        if 'tps' in stats:
            temp = re.findall(r'[\d.]+', stats['tps'])
            res = []
            for x in temp:
                try:
                    res.append(int(x))
                except ValueError:
                    res.append(float(x))

            assert len(res) == 11 and res[7] == 95

            stats_dict['tps'] = {
                'time':     res[0],
                'threads':  res[1],
                'tps':      res[2],
                'qps': {
                    'total': res[3],
                    'reads': res[4],
                    'writes':res[5],
                    'other': res[6],
                },
                'latency':  res[8],
                'errors':   res[9],
                'reconnects':res[10],
            }

        return stats_dict

    @classmethod
    def _parse_log(cls, filepath):

        sysbench_attributes = dict()

        with open(filepath) as fp:

            start_section = False
            section_name = None
            section_lines = []

            for line in fp.readlines():

                line = line.strip()

                # check if line with tps entry
                if re.match('[ [0-9]+s ]', line):
                    tps_end_line = line

                # section ended if line is empty
                elif start_section and not line:
                    sysbench_attributes[section_name] = section_lines
                    start_section = False
                    section_name = None
                    section_lines = []

                # if start_section then group lines
                elif start_section:
                    section_lines.append(line)

                else:
                    # check if start of a section
                    for section in cls.SECTIONS:
                        if section in line:
                            section_name = section
                            start_section = True
                            break


            sysbench_attributes['tps'] = tps_end_line

        return sysbench_attributes

a = SysbenchParseLogfile('/tmp/sql-sysbench.log')
pprint( a )
