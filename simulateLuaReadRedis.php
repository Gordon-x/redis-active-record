<?php
/**
 * Created by PhpStorm.
 * User: gordon
 * Date: 2017/9/21
 * Time: 上午10:59
 */

namespace RedisActiveRecord;


use yii\base\InvalidParamException;
use \yii\db\Exception;
use yii\base\NotSupportedException;
use yii\db\Expression;

/**
 * Class simulateLuaReadRedis
 * @package RedisActiveRecord
 */
class simulateLuaReadRedis
{
    /**
     * @var ActiveQuery
     */
    private $query;

    /**
     * @var string key
     */
    private $key;

    /**
     * 求和或取列的字段参数名；
     * @var
     */
    private $columnName;

    /**
     * 主键list
     * @var array
     */
    private $allPrimaryKeys;

    /**
     * 条件中的字段集合
     * @var array
     */
    private $columns = [];

    /**
     * 分页条件
     * @var string
     */
    private $limitCondition = '';

    /**
     * 条件语句
     * @var string
     */
    private $condition = '';

    /**
     * 排序顺序数组；
     * @var array
     */
    private $order = [];

    /**
     * 获取数据用于分页条件判断
     * @var int
     */
    private $i = 0;

    /**
     * 用于求和保存最终结果
     * @var int
     */
    private $sum = 0;

    /**
     * 用于计数记录数
     * @var int
     */
    private $count = 0;

    /**
     * 自增主键值
     * @var int
     */
    private $primaryKey = 0;

    public function __construct($query)
    {
        $this->query = $query;
        $this->reset();
        //生成key
        $this->initKey();
    }

    private function reset()
    {
        $this->key = '';
        $this->condition = '';
        $this->limitCondition = '';
        $this->columns = [];
        $this->columnName = '';
        $this->allPrimaryKeys = [];
        $this->i = 0;
        $this->sum = 0;
        $this->count = 0;
        $this->primaryKey = 0;
    }

    /**
     * 初始化key，表记录key的前缀；
     */
    private function initKey()
    {
        $this->key = $this->query->modelClass::keyPrefix() . ':a:';
    }

    /**
     * 设置求和（sum）或取列（column）的字段名；
     * @param string $columnName
     * @return $this
     */
    public function setColumnName($columnName = '')
    {
        $this->columnName = $columnName;
        return $this;
    }

    /**
     * 获取所有满足条件的记录
     * @return string
     */
    public function buildAll()
    {
        $this->build();
        return $this->fetchData('$this->query->modelClass::getDb()->executeCommand(\'HGETALL\', [$this->key . $this->primaryKey]);');
    }

    /**
     * 获取满足条件的第一条记录
     * @return mixed
     */
    public function buildOne()
    {
        $this->build();
        $data = $this->fetchData('$this->query->modelClass::getDb()->executeCommand(\'HGETALL\', [$this->key . $this->primaryKey]);return;');
        return current($data);
    }

    /**
     * 获取记录的条数
     * @return mixed
     */
    public function buildCount()
    {
        $this->build();
        return $this->fetchData('$this->count++;', 'return $this->count;');
    }

    /**
     * 对指定的字段求和
     * @return mixed
     */
    public function buildSum()
    {
        $this->build();
        $resultScript = '$this->query->modelClass::getDb()->executeCommand(\'HGET\', [$this->key . $this->primaryKey, $this->columnName]);';
        $return = 'foreach ($data as $v) {
            $this->sum += $v;
        }
        return $this->sum;';
        $data = $this->fetchData($resultScript, $return);
        return $data;
    }

    /**
     * 获取一列的所有数据
     * @return mixed
     */
    public function buildColumn()
    {
        $this->build();
        return $this->fetchData('$this->query->modelClass::getDb()->executeCommand(\'HGET\', [$this->key . $this->primaryKey, $this->columnName]);');
    }

    /**
     * 获取一列数据的最小值（未实现）
     * @throws NotSupportedException
     */
    public function buildMin()
    {
        throw new NotSupportedException('min not support');
    }

    /**
     * 获取一列的最大值（未实现）
     * @throws NotSupportedException
     */
    public function buildMax()
    {
        throw new NotSupportedException('max not support');
    }

    /**
     * 构建条件语句及条件字段集合，分页条件语句（不支持orderBy）
     * @throws NotSupportedException
     */
    private function build()
    {
        if (!empty($this->query->orderBy)) {
            $this->buildOrderBy();
        }

        if ($this->query->where !== null) {
            $this->condition = $this->buildCondition($this->query->where);
        } else {
            $this->condition = 'true';
        }

        $this->buildLimit();
    }

    private function buildOrderBy()
    {
        $orderBy = $this->query->orderBy;
        if (isset($orderBy[0]) && $orderBy[0] instanceof Expression) {
            throw new NotSupportedException('orderBy Expression is currently not supported by redis ActiveRecord.');
        }

        $this->order = $orderBy;
    }

    /**
     * 构建分页条件语句；
     */
    private function buildLimit()
    {
        $start = $this->query->offset === null ? 0 : $this->query->offset;
        $this->limitCondition = '$this->i >' . $start . ($this->query->limit === null ? '' : ' && $this->i <=' . ($start + $this->query->limit));
    }

    /**
     * 获取数据
     * @param $getResult string 获取数据的命令字符串
     * @param string $return 返回数据的命令字符串，不传则返回默认值
     * @return mixed 返回$return传入的命令字符串执行结果，默认返回redis事务执行结果，
     */
    private function fetchData($getResult, $return = '')
    {
        // 获取原始数据；
        $originData = $this->getAllByCondition();
        $modelClass = $this->query->modelClass;
        //事务开启；
        $modelClass::getDb()->multi();
        //取主键字段名
        $primaryKeyName = $modelClass::primaryKey();
        foreach ($originData as $value) {
            //字段名集合
            $columns = array_keys($this->columns);
            array_unshift($columns, $primaryKeyName[0]);
            $this->primaryKey = $value[0];
            //对条件字段赋值；
            foreach ($value as $k => $v) {
                ${$columns[$k]} = $v;
            }
            $condition = 'return ' . $this->condition . ';';
            //条件过滤
            if (eval($condition)) {
                $this->i++;
                $limit = 'return ' . $this->limitCondition . ';';
                //分页limit条件过滤
                if (eval($limit)) {
                    //取数据
                    eval($getResult);
                }
            }
        }
        //执行事务；
        $data = $modelClass::getDb()->exec();
        unset($modelClass);
        unset($originData);
        unset($primaryKeyName);
        return $return ? eval($return) : $data;
    }

    /**
     * 获取所有条件字段的数据
     * 无条件的情况返回主键
     * @return array
     */
    private function getAllByCondition()
    {
        $modelClass = $this->query->modelClass;
        $db = $modelClass::getDb();
        $pkName = $modelClass::primaryKey();

        $this->allPrimaryKeys = $db->lrange($modelClass::keyPrefix(), 0, -1);
        //主键倒序；
        if ($this->query->descPk) {
            $this->allPrimaryKeys = array_reverse($this->allPrimaryKeys);
        }

        $originData = [];
        //无条件语句的情况无需通过事务取数据；
        $this->query->where !== null ? $db->multi() : null;
        foreach ($this->allPrimaryKeys as $primaryKey) {
            if ($this->query->where !== null) {
                if (isset($this->columns[$pkName[0]])) {
                    unset($this->columns[$pkName[0]]);
                }
                $columns = array_keys($this->columns);
                array_unshift($columns, $pkName[0]);
                array_unshift($columns, $this->key . $primaryKey);
                $db->executeCommand('HMGET', $columns);
            } else {
                $originData[] = [$primaryKey];
            }
        }

        $data = $this->query->where !== null ? $db->exec() : $originData;
        unset($db);
        unset($pkName);
        unset($modelClass);
        unset($primaryKey);
        unset($this->allPrimaryKeys);
        return $data;
    }

    /**
     * 生成变量字符串
     * @param $name
     * @return string
     */
    private function varToStr($name)
    {
        return '$' . $name;
    }

    /**
     * 构建条件语句；
     * @param $condition
     * @return mixed|string
     * @throws Exception
     * @throws NotSupportedException
     */
    public function buildCondition($condition)
    {
        $builders = [
            'not' => 'buildNotCondition',
            'and' => 'buildAndCondition',
            'or' => 'buildAndCondition',
            'between' => 'buildBetweenCondition',
            'not between' => 'buildBetweenCondition',
            'in' => 'buildInCondition',
            'not in' => 'buildInCondition',
            'like' => 'buildLikeCondition',
            'not like' => 'buildLikeCondition',
            '>' => 'buildCompareCondition',
            '>=' => 'buildCompareCondition',
            '<' => 'buildCompareCondition',
            '<=' => 'buildCompareCondition',
        ];

        if (!is_array($condition)) {
            throw new NotSupportedException('Where condition must be an array in redis ActiveRecord.');
        }
        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
            $operator = strtolower($condition[0]);
            if (isset($builders[$operator])) {
                $method = $builders[$operator];
                array_shift($condition);

                return call_user_func_array([$this, $method], [$operator, $condition]);
            } else {
                throw new Exception('Found unknown operator in query: ' . $operator);
            }
        } else { // hash format: 'column1' => 'value1', 'column2' => 'value2', ...

            return $this->buildHashCondition($condition);
        }
    }

    /**
     * 值对条件创建，and连接；
     * @param $condition
     * @return mixed|string
     * @throws NotSupportedException
     */
    private function buildHashCondition($condition)
    {
        $parts = [];
        foreach ($condition as $column => $value) {
            if (is_array($value)) { // IN condition
                $parts[] = $this->buildInCondition('in', [$column, $value]);
            } else {
                if (is_bool($value)) {
                    $value = (int)$value;
                }
                if ($value === null) {
                    throw new NotSupportedException('null value is not support');
                } elseif ($value instanceof Expression) {
                    $column = $this->addColumn($column);
                    $parts[] = $this->varToStr($column) . "==" . $value->expression;
                } else {
                    $column = $this->addColumn($column);
                    $value = $this->quoteValue($value);
                    $parts[] = $this->varToStr($column) . "==$value";
                }
            }
        }

        return count($parts) === 1 ? $parts[0] : '(' . implode(') && (', $parts) . ')';
    }

    /**
     * 非条件语句；
     * @param $operator
     * @param $operands
     * @return string
     */
    private function buildNotCondition($operator, $operands)
    {
        if (count($operands) != 1) {
            throw new InvalidParamException("Operator '$operator' requires exactly one operand.");
        }

        $operand = reset($operands);
        if (is_array($operand)) {
            $operand = $this->buildCondition($operand);
        }

        return "!($operand)";
    }

    /**
     * and连接条件
     * @param $operator
     * @param $operands
     * @return string
     */
    private function buildAndCondition($operator, $operands)
    {
        $parts = [];
        foreach ($operands as $operand) {
            if (is_array($operand)) {
                $operand = $this->buildCondition($operand);
            }
            if ($operand !== '') {
                $parts[] = $operand;
            }
        }

        if (!empty($parts)) {
            return '(' . implode(") $operator (", $parts) . ')';
        } else {
            return '';
        }
    }

    /**
     * between 条件；
     * @param $operator
     * @param $operands
     * @return string
     * @throws Exception
     */
    private function buildBetweenCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1], $operands[2])) {
            throw new Exception("Operator '$operator' requires three operands.");
        }

        list($column, $value1, $value2) = $operands;

        $value1 = $this->quoteValue($value1);
        $value2 = $this->quoteValue($value2);
        $column = $this->addColumn($column);

        return $this->varToStr($column) . " >= $value1 && " . $this->varToStr($column) . " <= $value2";
    }

    /**
     * 比较条件；
     * @param $operator
     * @param $operands
     * @return string
     * @throws Exception
     */
    private function buildCompareCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new Exception("Operator '$operator' requires two operands.");
        }

        list($column, $value) = $operands;

        $value = $this->quoteValue($value);
        $column = $this->addColumn($column);
        return '(' . $this->varToStr($column) . " $operator $value )";
    }

    /**
     * IN || NOT IN 条件；
     * @param $operator
     * @param $operands
     * @return string
     * @throws Exception
     * @throws NotSupportedException
     */
    private function buildInCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new Exception("Operator '$operator' requires two operands.");
        }

        list($column, $values) = $operands;

        $values = (array)$values;

        if (empty($values) || $column === []) {
            return $operator === 'in' ? 'false' : 'true';
        }

        if (count($column) > 1) {
            return $this->buildCompositeInCondition($operator, $column, $values);
        } elseif (is_array($column)) {
            $column = reset($column);
        }
        $columnAlias = $this->addColumn($column);
        $parts = [];
        foreach ($values as $value) {
            if (is_array($value)) {
                $value = isset($value[$column]) ? $value[$column] : null;
            }
            if ($value === null) {
                throw new NotSupportedException('null value is not support');
            } elseif ($value instanceof Expression) {
                $parts[] = $this->varToStr($columnAlias) . "==" . $value->expression;
            } else {
                $value = $this->quoteValue($value);
                $parts[] = $this->varToStr($columnAlias) . "==$value";
            }
        }
        $operator = $operator === 'in' ? '' : ' !';

        return "$operator(" . implode(' || ', $parts) . ')';
    }

    /**
     * @param $operator
     * @param $inColumns
     * @param $values
     * @return string
     * @throws Exception
     */
    protected function buildCompositeInCondition($operator, $inColumns, $values)
    {
        $vss = [];
        foreach ($values as $value) {
            $vs = [];
            foreach ($inColumns as $column) {
                if (isset($value[$column])) {
                    $columnAlias = $this->addColumn($column);
                    $vs[] = $this->varToStr($columnAlias) . "==" . $this->quoteValue($value[$column]);
                } else {
                    throw new Exception('value is not set');
                }
            }
            $vss[] = '(' . implode(' && ', $vs) . ')';
        }
        $operator = $operator === 'in' ? '' : ' !';

        return "$operator(" . implode(' || ', $vss) . ')';
    }

    /**
     * like 条件（暂不支持）
     * @param $operator
     * @param $operands
     * @throws NotSupportedException
     */
    private function buildLikeCondition($operator, $operands)
    {
        throw new NotSupportedException('LIKE conditions are not suppoerted by redis ActiveRecord.');
    }

    /**
     * Adds a column to the list of columns to retrieve and creates an alias
     * @param string $column the column name to add
     * @return string the alias generated for the column name
     */
    private function addColumn($column)
    {
        if (isset($this->columns[$column])) {
            return $this->columns[$column];
        }
        $name = preg_replace("/[^A-z]+/", "", $column);

        return $this->columns[$column] = $name;
    }

    /**
     * Quotes a string value for use in a query.
     * Note that if the parameter is not a string or int, it will be returned without change.
     * @param string $str string to be quoted
     * @return string the properly quoted string
     */
    private function quoteValue($str)
    {
        if (!is_string($str) && !is_int($str)) {
            return $str;
        }

        return "'" . addcslashes($str, "\000\n\r\\\032\047") . "'";
    }
}
