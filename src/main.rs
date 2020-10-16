use std::sync::Arc;

use arrow::array::{
    Array, ArrayData, BooleanArray, Int32Array, Int32Builder, ListArray, PrimitiveArray,
    StringArray, StructArray, StringBuilder, ArrayBuilder, StructBuilder
};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Date64Type, Field, Time64MicrosecondType, ToByteSlice};

fn main() {
    // Primitive types
    //
    // Lets define an array builder with a capacity of 20.  This builder is going to be used to
    // create and Arrow array holding u32 integers.  The Arrow specification defines several
    // primitive datatypes that the Rust implementation uses to define the conversion between a Rust
    // native datatypes and Arrow datatypes.  For more information refer to the arrow::datatypes
    let mut primitive_array_builder = Int32Builder::new(20);

    // By appending values to the builder, which behind the scene has a variable buffer, one can
    // store as many values as required. If by any chance there are more values that the previously
    // estimated builder capacity, then the builder adjusts its buffer size to fit the appended
    // values
    primitive_array_builder.append_value(5).unwrap();
    primitive_array_builder.append_value(10000).unwrap();
    primitive_array_builder.append_value(2000000).unwrap();
    primitive_array_builder.append_null().unwrap();
    primitive_array_builder.append_slice(&[1, 2, 3]).unwrap();
    primitive_array_builder.append_null().unwrap();
    primitive_array_builder
        .append_slice(&(0..10).collect::<Vec<i32>>())
        .unwrap();

    // Once all the data has been added to the builder, the array can be created by finishing the
    // array_builder
    let primitive_array = primitive_array_builder.finish();
    println!("{:?}", primitive_array);

    // It should be noted that all the data from the builder is "transferred" to the array, leaving
    // the builder empty
    let primitive_array_2 = primitive_array_builder.finish();
    println!("{:?}", primitive_array_2);

    // The builder can be used again to create a new array by appending data to it and finishing it
    // once all the required data has been added.
    primitive_array_builder.append_value(1).unwrap();
    primitive_array_builder.append_null().unwrap();
    primitive_array_builder.append_slice(&[1, 2, 3]).unwrap();
    primitive_array_builder.append_null().unwrap();

    let primitive_array_3 = primitive_array_builder.finish();
    println!("{:?}", primitive_array_3);

    // The array buffer
    //
    // Now, it should be remembered that an Arrow array is constructed using a buffer that contains
    // all the data stored when it was constructed. The buffered values can be seen by looking at
    // the array buffer
    println!("{:?}", primitive_array.values());

    // If you have been following along this example and printed the previous line you would have
    // noticed that the buffer is not showing the values as they were appended.  As a matter of
    // fact there are some zeros between the numbers that were never appended.  These zeros
    // represent the padded arrangement among the numbers that make the array.  One of the Arrow
    // specifications indicates that the numbers should be aligned using an u8 pointer. This means
    // that in order to represent a u32 integer the buffer has to allocate 4 bytes or 4 u8s. That
    // explains the extra zeros that can be seen when printing the buffer. Also, if the array
    // was made out of u64 the buffer would have to allocate 8 bytes to store each number.
    //
    // Just as a small example of the inner workings of the buffer, lets use the raw buffer pointer
    // to access the information inside and how it can be casted to represent the appended value
    let buffer = primitive_array.values();
    println!("u8 values");
    println!("{:?}", buffer.raw_data());
    unsafe {
        for i in 0..10 {
            println!("\t{:?}", *buffer.raw_data().add(i));
        }
    }

    // By recasting the buffer's pointer to a u32 pointer, the stored values can be seen as u32
    // numbers without the padding.
    let u32_ptr = buffer.raw_data() as *const u32;
    println!("u32 values");
    unsafe {
        for i in 0..10 {
            println!("\t{:?}", *u32_ptr.add(i));
        }
    }
    // It should be mentioned that the use of raw pointers is not necessary to work with the Arrow
    // crate. All the pointer operations are done safely within the crate.  However, it is
    // important to give a small explanation of the pointer arithmetic that is done behind the
    // crate and how the buffer manages the data that represents an Arrow array.

    // Comming back to creating arrays. There are other ways to create arrays from native
    // Rust datatypes. For example, an array can be created by using the Into trait and using the
    // Option enum
    let date_array: PrimitiveArray<Date64Type> =
        vec![Some(1550902545147), None, Some(1550902545147)].into();
    println!("{:?}", date_array);

    let time_array: PrimitiveArray<Time64MicrosecondType> = (0..100).collect::<Vec<i64>>().into();
    println!("{:?}", time_array);

    // Nested types
    //
    // So far only primitive arrays have been created. These primitive arrays, as their name
    // indicates, are made out of primitive datatypes. They don't have children or parent types.
    // However, the Arrow specification has nested array types. These nested arrays are structures
    // that depend on one or more child types. By using nested arrays one can represent arrays of
    // variable size, structs, sparse and dense unions or null sequences. For further detail
    // regarding the elements that compose these arrays (buffers, offsets, values, etc), the Arrow
    // Columnar Format documentation has a clear explanation of these components.
    //
    // Then, in order to create a nested array it is important to define the children arrays that
    // will define the data array. Lets define an ArrayData that will represent the array ["hello",
    // null, "parquet"]. This data array will have to be defined by a value array, and offset array
    // and a validity array.

    let values: [u8; 12] = [
        b'h', b'e', b'l', b'l', b'o', b'p', b'a', b'r', b'q', b'u', b'e', b't',
    ];

    let offsets: [i32; 4] = [0, 5, 5, 12];

    // The string array will be created using a generic ArrayData struct which will be converted to
    // a String array. The generic array is put together using an offset buffer, a values buffer
    // and a validity array. By the way, notice the order each buffer is added to the ArrayData.
    // Each buffer is stored in a vector of buffers, so the only reference other constructors will
    // have is the vector index.
    let array_data = ArrayData::builder(DataType::Utf8)
        .len(3)
        .add_buffer(Buffer::from(offsets.to_byte_slice()))
        .add_buffer(Buffer::from(&values[..]))
        .null_bit_buffer(Buffer::from([0b00000101]))
        .build();

    // Again, if the generic ArrayData is printed only the buffers will be printed, with no
    // specific representation for the data.
    println!("{:?}", array_data);

    // From this generic array a StringArray can be created. The StringArray now represents the
    // strings that want to be stored in an Arrow Array.
    let binary_array = StringArray::from(array_data);
    println!("{:?}", binary_array);

    // ListArray
    //
    // A similar process can be followed to create an array of lists or a ListArray. In order to
    // create a representation of this array:
    //     array = [[0, 1, 2], [3, 4, 5], [6, 7]]
    // A generic ArrayData is created with the values that are going to be stored in the final
    // Arrow array.
    let value_data = ArrayData::builder(DataType::Int32)
        .len(8)
        .add_buffer(Buffer::from(&[0, 1, 2, 3, 4, 5, 6, 7, 8].to_byte_slice()))
        .build();

    let value_offsets = Buffer::from(&[0, 3, 6, 8].to_byte_slice());

    // Before creating the array that will represent the array, another generic ArrayData needs to
    // be created. However, this new generic array will be composed of the value data and the
    // offset buffer. In this case the ListArray constructor depends on the existence of child data
    // in the generic array.
    let list_data_type = DataType::List(Box::new(DataType::Int32));
    let list_data = ArrayData::builder(list_data_type)
        .len(3)
        .add_buffer(value_offsets)
        .add_child_data(value_data)
        .build();
    println!("{:?}", list_data);

    let list_array = ListArray::from(list_data);
    println!("{:?}", list_array);

    // StructArray
    //
    // StructArrays are arrays of tuples, where each tuple element is from a child array. (In other
    // words, they're like zipping multiple columns into one and giving each subcolumn a label.)
    // StructArrays can be constructed using the StructArray::from helper, which takes the
    // underlying arrays and field types.
    let struct_array = StructArray::from(vec![
        (
            Field::new("b", DataType::Boolean, false),
            Arc::new(BooleanArray::from(vec![false, false, true, true])) as Arc<dyn Array>,
        ),
        (
            Field::new("c", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![42, 28, 19, 31])),
        ),
    ]);
    println!("{:?}", struct_array);

    // Constructing an StructArray from generic ArrayData
    let boolean_data = ArrayData::builder(DataType::Boolean)
        .len(5)
        .add_buffer(Buffer::from([0b00010000]))
        .null_bit_buffer(Buffer::from([0b00010001]))
        .build();

    let int_data_b = ArrayData::builder(DataType::Int32)
        .len(5)
        .add_buffer(Buffer::from([0, 28, 42, 0, 0].to_byte_slice()))
        .null_bit_buffer(Buffer::from([0b00000110]))
        .build();

    let int_data_c = ArrayData::builder(DataType::Int32)
        .len(5)
        .add_buffer(Buffer::from([1, 2, 3, 4, 5].to_byte_slice()))
        .null_bit_buffer(Buffer::from([0b00011111]))
        .build();

    let mut field_types = vec![];
    field_types.push(Field::new("a", DataType::Boolean, false));
    field_types.push(Field::new("b", DataType::Int32, false));
    field_types.push(Field::new("c", DataType::Int32, false));

    let struct_array_data = ArrayData::builder(DataType::Struct(field_types))
        .len(5)
        .add_child_data(boolean_data.clone())
        .add_child_data(int_data_b.clone())
        .add_child_data(int_data_c.clone())
        .null_bit_buffer(Buffer::from([0b00000000]))
        .build();
    let struct_array = StructArray::from(struct_array_data);

    println!("{:?}", struct_array);
}
