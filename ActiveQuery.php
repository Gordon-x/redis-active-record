<?php
/**
 * Created by PhpStorm.
 * User: gordon
 * Date: 2017/9/20
 * Time: 下午7:27
 */

namespace RedisActiveRecord;


use yii\base\NotSupportedException;
use yii\db\ActiveQueryTrait;
use yii\db\ActiveRelationTrait;
use yii\db\QueryTrait;
use yii\redis\ActiveRecord;
use yii\redis\Connection;
use yii\base\InvalidParamException;
use yii\redis\ActiveQuery as YiiActiveQuery;

/**
 * Class ActiveQuery
 * @package RedisActiveRecord
 *
 * @property ActiveRecord $modelClass
 */
class ActiveQuery extends YiiActiveQuery
{
    use QueryTrait;
    use ActiveQueryTrait;
    use ActiveRelationTrait;

    /**
     * 倒序主键；
     * @var bool
     */
    public $descPk = false;

    /**
     * @var simulateLuaReadRedis
     */
    private $simulateLuaReadRedis;

    public function init()
    {
        parent::init();
        //模拟lua读取redis
        $this->simulateLuaReadRedis = new simulateLuaReadRedis($this);
    }

    /**
     * 倒序主键；
     * @return $this
     */
    public function descPk()
    {
        $this->descPk = true;
        return $this;
    }

    /**
     * 执行脚本；
     * @param null|Connection $db
     * @param string $type
     * @param null $columnName
     * @return array|bool|null|string
     * @throws NotSupportedException
     */
    public function executeScript($db, $type, $columnName = null)
    {
        if ($this->primaryModel !== null) {
            // lazy loading
            if ($this->via instanceof self) {
                // via junction table
                $viaModels = $this->via->findJunctionRows([$this->primaryModel]);
                $this->filterByModels($viaModels);
            } elseif (is_array($this->via)) {
                // via relation
                /* @var $viaQuery YiiActiveQuery */
                list($viaName, $viaQuery) = $this->via;
                if ($viaQuery->multiple) {
                    $viaModels = $viaQuery->all();
                    $this->primaryModel->populateRelation($viaName, $viaModels);
                } else {
                    $model = $viaQuery->one();
                    $this->primaryModel->populateRelation($viaName, $model);
                    $viaModels = $model === null ? [] : [$model];
                }
                $this->filterByModels($viaModels);
            } else {
                $this->filterByModels([$this->primaryModel]);
            }
        }

        if (!empty($this->orderBy)) {
            throw new NotSupportedException('orderBy is currently not supported by redis ActiveRecord.');
        }

        /* @var $modelClass ActiveRecord */
        $modelClass = $this->modelClass;

        if ($db === null) {
            $db = $modelClass::getDb();
        }

        // find by primary key if possible. This is much faster than scanning all records
        if (is_array($this->where) && (
                !isset($this->where[0]) && $modelClass::isPrimaryKey(array_keys($this->where)) ||
                isset($this->where[0]) && $this->where[0] === 'in' && $modelClass::isPrimaryKey((array) $this->where[1])
            )) {
            return $this->findByPk($db, $type, $columnName);
        }

        $method = 'build' . $type;
        //获取原始数据；
        $result = $this->simulateLuaReadRedis
            ->setColumnName($columnName)
            ->$method();
        return $result;
    }

    /**
     * Fetch by pk if possible as this is much faster
     * @param Connection $db the database connection used to execute the query.
     * If this parameter is not given, the `db` application component will be used.
     * @param string $type the type of the script to generate
     * @param string $columnName
     * @return array|bool|null|string
     * @throws \yii\base\InvalidParamException
     * @throws \yii\base\NotSupportedException
     */
    private function findByPk($db, $type, $columnName = null)
    {
        if (isset($this->where[0]) && $this->where[0] === 'in') {
            $pks = (array) $this->where[2];
        } elseif (count($this->where) == 1) {
            $pks = (array) reset($this->where);
        } else {
            foreach ($this->where as $values) {
                if (is_array($values)) {
                    // TODO support composite IN for composite PK
                    throw new NotSupportedException('Find by composite PK is not supported by redis ActiveRecord.');
                }
            }
            $pks = [$this->where];
        }

        /* @var $modelClass ActiveRecord */
        $modelClass = $this->modelClass;

        if ($type == 'Count') {
            $start = 0;
            $limit = null;
        } else {
            $start = $this->offset === null ? 0 : $this->offset;
            $limit = $this->limit;
        }
        $i = 0;
        $data = [];
        foreach ($pks as $pk) {
            if (++$i > $start && ($limit === null || $i <= $start + $limit)) {
                $key = $modelClass::keyPrefix() . ':a:' . $modelClass::buildKey($pk);
                $result = $db->executeCommand('HGETALL', [$key]);
                if (!empty($result)) {
                    $data[] = $result;
                    if ($type === 'One' && $this->orderBy === null) {
                        break;
                    }
                }
            }
        }
        // TODO support orderBy

        switch ($type) {
            case 'All':
                return $data;
            case 'One':
                return reset($data);
            case 'Count':
                return count($data);
            case 'Column':
                $column = [];
                foreach ($data as $dataRow) {
                    $row = [];
                    $c = count($dataRow);
                    for ($i = 0; $i < $c;) {
                        $row[$dataRow[$i++]] = $dataRow[$i++];
                    }
                    $column[] = $row[$columnName];
                }

                return $column;
            case 'Sum':
                $sum = 0;
                foreach ($data as $dataRow) {
                    $c = count($dataRow);
                    for ($i = 0; $i < $c;) {
                        if ($dataRow[$i++] == $columnName) {
                            $sum += $dataRow[$i];
                            break;
                        }
                    }
                }

                return $sum;
            case 'Average':
                $sum = 0;
                $count = 0;
                foreach ($data as $dataRow) {
                    $count++;
                    $c = count($dataRow);
                    for ($i = 0; $i < $c;) {
                        if ($dataRow[$i++] == $columnName) {
                            $sum += $dataRow[$i];
                            break;
                        }
                    }
                }

                return $sum / $count;
            case 'Min':
                $min = null;
                foreach ($data as $dataRow) {
                    $c = count($dataRow);
                    for ($i = 0; $i < $c;) {
                        if ($dataRow[$i++] == $columnName && ($min == null || $dataRow[$i] < $min)) {
                            $min = $dataRow[$i];
                            break;
                        }
                    }
                }

                return $min;
            case 'Max':
                $max = null;
                foreach ($data as $dataRow) {
                    $c = count($dataRow);
                    for ($i = 0; $i < $c;) {
                        if ($dataRow[$i++] == $columnName && ($max == null || $dataRow[$i] > $max)) {
                            $max = $dataRow[$i];
                            break;
                        }
                    }
                }

                return $max;
        }
        throw new InvalidParamException('Unknown fetch type: ' . $type);
    }
}
