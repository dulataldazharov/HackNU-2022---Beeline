import React, {FC, useEffect, useState, useMemo} from 'react';
import FadeIn from 'react-fade-in';
import LineTo from 'react-lineto';
import Beeline from '../assets/beeline.png';

const StatusBar: FC<{ statusBarColor: string, text: string, size: number, ref?: any }> = ({ statusBarColor, text, size, ref }) => (
    <div ref={ref} style={{ backgroundColor: statusBarColor, width: size, height: 48 }} className="flex justify-center items-center rounded-sm">
        <p className="text-sm" style={{ maxWidth: 150, overflow: 'hidden', whiteSpace: 'nowrap' }}>
            {text}
        </p>
    </div>
)

type Dependency = Record<string, Array<{ title: string }>>;

function Home() {
    const [rawDataSources, setRawDataSources] = useState({
        COLUMN_1: {
            data_sources: ["data_source1.csv", "data_source2.csv", "data_source3.csv", "data_source4.csv", "data_source5.csv"],
            cols_deps: ["data_source1.ID", "data_source2.SERIAL_NUMBER", "data_source2.MONTHLY_BILLING", "data_source2.MONTHLY_TARIFF", "data_source2.MONTHLY_INTERNET", "data_source2.MONTHLY_LTE", "data_source2.MONTHLY_BILLING_2", "data_source2.MONTHLY_BILLING", "data_source2.MONTHLY_TARIFF", "data_source2.MONTHLY_INTERNET", "data_source2.MONTHLY_LTE", "data_source2.MONTHLY_BILLING_2"],
        },
        COLUMN_2: {
            data_sources: ["data_source1.csv", "data_source2.csv"],
            cols_deps: ["data_source1.ID", "data_source2.SERIAL_NUMBER"],
        },
        COLUMN_3: {
            data_sources: ["data_source1.csv", "data_source2.csv"],
            cols_deps: ["data_source1.ID", "data_source2.SERIAL_NUMBER"],
        },
        non_trivial_cols_deps: ["data_source3.FILTERED", "data_source3.FILTERED_2"]
    });
    const [dataSources, setDataSources] = useState<string[]>([]);
    const [finalColumns, setFinalColumns] = useState<string[]>(Object.keys(rawDataSources));
    const [initialColumns, setInitialColumns] = useState<string[]>(Object.keys(rawDataSources));
    const [_, setChineseRender] = useState(false);
    const [columnName, setColumnName] = useState("");
    const [jsonValue, setJsonValue] = useState("");

    const dependencyMap: Dependency = useMemo(() => {
        const adjGraph: Dependency = {};

        Object.keys(rawDataSources).forEach((key) => {
            // @ts-ignore
            const data = rawDataSources[key] || {};
            adjGraph[key] = [];

            if (data?.cols_deps) {
                data?.cols_deps?.forEach((column: string) => {
                    adjGraph[key].push({ title: column });
                });
            }
        });

        return adjGraph;
    }, [rawDataSources]);

    useEffect(() => {
        setTimeout(() => {
            setChineseRender(true);
        }, 50);

        const set = new Set<string>();
        const initialCols = new Set<string>();
        const finalCols = new Set<string>();
        Object.keys(rawDataSources).forEach((key) => {
            // @ts-ignore
            const data = rawDataSources[key] || {};
            if (key !== 'non_trivial_cols_deps') {
                finalCols.add(key);
            }

            if (data?.data_sources) {
                data?.data_sources?.forEach((data_source: string) => {
                    if (data_source) {
                        set.add(data_source);
                    }
                })
            }

            if (data?.cols_deps) {
                data?.cols_deps?.forEach((column: string) => {
                    initialCols.add(column);
                });
            }
        });

        rawDataSources?.non_trivial_cols_deps?.forEach((column) => {
            initialCols.add(column);
        });

        const tablesArray: string[] = Array.from(set);
        const initialColumnsArray: string[] = Array.from(initialCols);
        const finalColumnsArray: string[] = Array.from(finalCols);
        setDataSources(tablesArray);
        setInitialColumns(initialColumnsArray);
        setFinalColumns(finalColumnsArray);
    }, [rawDataSources]);

    const dependencyPairs: Array<{ left: string, right: string }> = useMemo(() => {
        const pairs: Array<{ left: string, right: string }> = [];

        if (dependencyMap[columnName]) {
            const dependencies = dependencyMap[columnName];

            dependencies?.forEach((nodeDep) => {
                pairs.push({left: columnName, right: nodeDep.title});
            })
        }

        return pairs;
    }, [dependencyMap, columnName]);

    const filteredDependencyPairs: Array<{ left: string, right: string }> = useMemo(() => {
        const pairs: Array<{ left: string, right: string }> = [];

        const filteredSources = rawDataSources?.["non_trivial_cols_deps"];
        filteredSources?.forEach((filteredColumn) => {
            pairs.push({ left: columnName, right: filteredColumn });
        });

        return pairs;
    }, [rawDataSources, columnName]);

    const handleJSONChange = (e: any) => {
        setJsonValue(e.target.value);
    }

    const handleColumnChange = (e : any) => {
        setColumnName(e.target.value);
    }

    const handleApply = () => {
        setRawDataSources(JSON.parse(jsonValue));
    }

    return (
        <div className="mb-64">
            <div className="flex justify-center mb-8">
                <div style={{ width: 180, height: 72 }} className="bg-white rounded-md px-4 py-2">
                    <img alt="logo" src={Beeline} style={{ height: 56 }} />
                </div>
            </div>

            <div className="flex items-center gap-12 mt-16 mb-10">
                <input type="text"
                       className="bg-gray-50 border border-gray-300 text-gray-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500"
                       placeholder="Enter JSON of dependencies"
                       value={jsonValue}
                       onChange={handleJSONChange}
                />
                <button
                    className="bg-transparent hover:bg-white text-white font-semibold hover:text-black py-2 px-4 border border-white hover:border-transparent rounded"
                    onClick={handleApply}>
                    Apply
                </button>
            </div>

            <FadeIn delay={200}>
                <p className="text-xl font-bold text-white mb-6">
                    Tables
                </p>
                <div className="flex items-center gap-6 mb-6">
                    <StatusBar text="Data Sources" statusBarColor='white' size={200} />
                    {
                        dataSources.map((title, i) => {
                            return (
                                <StatusBar key={i} text={title} statusBarColor='#B1ED4A' size={200} />
                            )
                        })
                    }
                </div>

                <input type="text"
                       className="mt-16 bg-gray-50 border border-gray-300 text-gray-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500"
                       placeholder="Enter column name to view dependencies"
                       onChange={handleColumnChange}
                />

                <p className="text-xl font-bold text-white my-6">
                    Column dependencies
                </p>

                <div className="grid grid-cols-2">
                    <div className="flex flex-col gap-8">
                        <StatusBar text="Final columns" statusBarColor='white' size={350} />
                        {
                            finalColumns.map((title, i) => {
                                return (
                                    <div key={i} className="flex">
                                        <div style={{ backgroundColor: '#B1ED4A', width: 350, height: 48 }} className="flex justify-center items-center rounded-sm">
                                            <p className="text-sm">
                                                {title}
                                            </p>
                                        </div>
                                        <div className={title}/>
                                    </div>
                                )
                            })
                        }
                    </div>

                    <div className="flex flex-col items-end gap-8">
                        <StatusBar text="Initial columns" statusBarColor='white' size={350} />
                        {
                            initialColumns.map((title, i) => {
                                const contents = title.split(".");
                                const tableName = contents.slice(0, contents.length-1).join(".");
                                const columnName = contents[contents.length-1];

                                return (
                                    <div key={i} className="flex">
                                        <div className={title} />
                                        <div style={{ backgroundColor: '#ED4A4A', width: 350, height: 48 }} className="flex justify-between items-center rounded-sm p-4 ">
                                            <p className="text-sm" style={{ maxWidth: 150, overflow: 'hidden', whiteSpace: 'nowrap' }}>
                                                {columnName}
                                            </p>
                                            <p className="text-sm" style={{ maxWidth: 150, overflow: 'hidden', whiteSpace: 'nowrap' }}>
                                                {tableName}
                                            </p>
                                        </div>
                                    </div>
                                )
                            })
                        }
                    </div>
                </div>
            </FadeIn>

            {
                dependencyPairs.map((pair, i) => (
                    <LineTo key={`${pair.left}-${pair.right}-${i}`} from={pair.left} to={pair.right} borderColor='white' borderWidth={1.5} />
                ))
            }

            {
                filteredDependencyPairs.map((pair, i) => (
                    <LineTo key={`${pair.left}-${pair.right}-${i}`} from={pair.left} to={pair.right} borderColor='#D800A6' borderStyle="dashed" borderWidth={2} />
                ))
            }
        </div>
    )
}

export default Home
