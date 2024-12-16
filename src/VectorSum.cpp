/* Copyright (c) DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2024 -*- C++ -*- */
/*
 * Description: User Defined Aggregate Function for vector_sum
 *
 * Create Date: Dec. 12, 2024
 */

#include "Vertica.h"
#include "Arrays/Accessors.h"

using namespace Vertica;
using namespace std;

class VectorSum : public AggregateFunction
{
    virtual void initAggregate(ServerInterface &srvInterface,
                       IntermediateAggs &aggs)
    {
        // Initialize the IR buffers
        VString &arrayIR = aggs.getStringRef(0);
        arrayIR.alloc(arrayIR.max_size);

        vint &arraySize = aggs.getIntRef(1);
        arraySize = 0;
    }

    inline void sum(VString &tgt, vint &tgtSize, const VString &frm)
    {
        vfloat *tgtBuffer = reinterpret_cast<vfloat *> (tgt.data());
        const vfloat *frmBuffer = reinterpret_cast<const vfloat *> (frm.data());
        const vsize frmLen = frm.length();

        int cnt = 0;
        int len = 0;
        while((tgtSize == 0 || len < tgtSize) && (len < frmLen))
        {
            //init arrayIR according to the input array
            if(tgtSize == 0)
                tgtBuffer[cnt] = vfloat_null;

            vfloat value = frmBuffer[cnt];
            if(!vfloatIsNull(value))
            {
                if(vfloatIsNull(tgtBuffer[cnt]))
                    tgtBuffer[cnt] = value;
                else
                    tgtBuffer[cnt] += value;
            }

            cnt++;
            len += sizeof(vfloat);
        }

        //set arraySize according to the input array
        //TODO: how to get the exact size of input array before Array::ArrayReader workable for AggregateFunction?
        if(tgtSize == 0)
            tgtSize = len;
    }


    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs)
    {
        // Get the running IR-buffers for this aggregate group
        VString &arrayIR = aggs.getStringRef(0);
        vint &arraySize = aggs.getIntRef(1);

        do
        {
            //Note: use VString API except argReader.getArrayRef(0),
            //      Array::ArrayReader does not work for AggregateFunction yet at least before 24.4
            const VString &otherIR = argReader.getStringRef(0);

            sum(arrayIR, arraySize, otherIR);
        } while(argReader.next());
    }


    virtual void combine(ServerInterface &srvInterface,
                 IntermediateAggs &aggs,
                 MultipleIntermediateAggs &aggsOther)
    {
        // Get the IR buffers for a specific aggregate group and combine it with the ones from other groups
        VString &arrayIR = aggs.getStringRef(0);
        vint &arraySize = aggs.getIntRef(1);
        do
        {
            const VString &otherIR = aggsOther.getStringRef(0);
            sum(arrayIR, arraySize, otherIR);
        } while(aggsOther.next());
    }


    virtual void terminate(ServerInterface &srvInterface,
                   BlockWriter &resWriter,
                   IntermediateAggs &aggs)
    {
        const VString &arrayIR = aggs.getStringRef(0);
        vint &arraySize = aggs.getIntRef(1);

        VString &resultBuffer = resWriter.getStringRef();
        resultBuffer.copy(arrayIR.data(), arraySize);
    }

    using AggregateFunction::terminate;

    InlineAggregate()
};


class VectorSumFactory : public AggregateFunctionFactory
{
    virtual AggregateFunction *createAggregateFunction(ServerInterface &interface)
    {
        return vt_createFuncObject<VectorSum>(interface.allocator);
    }


    virtual void getPrototype(ServerInterface &srvInterface, 
                              ColumnTypes &argTypes, 
                              ColumnTypes &returnType)
    {
        ColumnTypes typeFloat;
        typeFloat.addFloat();

        argTypes.addArrayType(typeFloat);
        returnType.addArrayType(typeFloat);
    }


    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnTypes)
    {
        returnTypes.addArg(argTypes.getColumnType(0));
    }


    virtual void getIntermediateTypes(ServerInterface &srvInterface, 
                              const SizedColumnTypes &inputTypes,
                              SizedColumnTypes &intermediateTypeMetaData)
    {
        // array size according to the definition of the input column,
        // Note, it may be wider than real input values
        intermediateTypeMetaData.addArg(inputTypes.getColumnType(0), "buffer");

        // real output vector size
        intermediateTypeMetaData.addInt();
    }
};


RegisterFactory(VectorSumFactory);
