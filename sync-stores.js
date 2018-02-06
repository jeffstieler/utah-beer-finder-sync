'use strict';

const config = require( 'config' );
const DABC = require( './dabc' );
const WooCommerceAPI = require( 'woocommerce-api' );
const UntappdClient = require( 'node-untappd' );
const querystring = require( 'querystring' );
const PromisePool = require( 'promise-pool-executor' );
const _ = require( 'lodash' );

const WooCommerce = new WooCommerceAPI( {
    url: config.get( 'woocommerce.url' ),
    consumerKey: config.get( 'woocommerce.key' ),
    consumerSecret: config.get( 'woocommerce.secret' ),
    wpAPI: true,
    version: 'wp/v2',
} );

const getStoreMapByNumber = ( number ) => new Promise( ( resolve, reject ) => {
    WooCommerce.get(
        'stores?' + querystring.stringify( { slug: `store-${number}` } ),
        ( err, res, body ) => {
            if ( err ) {
                reject( err );
            } else {
                try {
                    const json = JSON.parse( body );

                    resolve( json );
                } catch ( syntaxError ) {
                    console.error( body );

                    reject( syntaxError );
                }
            }
        }
    );
} );

const addNewStore = ( storeToCreate ) => new Promise( ( resolve, reject ) => {
    const storeMapData = {
        name: storeToCreate.label,
        latitude: storeToCreate.latitude,
        longitude: storeToCreate.longitude,
        info_window: [
            `<strong>Address:</strong> ${storeToCreate.address01}<br/>${storeToCreate.address02}`,
            `<strong>Phone:</strong> ${storeToCreate.phone}`,
            `<strong>Manager:</strong> ${storeToCreate.manager}`,
            `<strong>Store Hours:</strong> ${storeToCreate.hours}`,
        ].join( '<br/>' ),
    };

    console.log( `Creating map for ${storeToCreate.label}` );

    WooCommerce.post(
        'stores',
        storeMapData,
        ( err, res, body ) => {
            if ( err ) {
                reject( err );
            } else {
                try {
                    const json = JSON.parse( body );

                    resolve( json );
                } catch ( syntaxError ) {
                    console.error( body );

                    reject( syntaxError );
                }
            }
        }
    );
} );

DABC.getAllStores( ( err, stores ) => {
    console.log( `Found ${stores.length} stores, syncing..` );
    
    // Create a pool with a concurrency limit
    const pool = new PromisePool.PromisePoolExecutor( {
        frequencyLimit: 10,
        frequencyWindow: 1200,
    } );

    pool.addEachTask( {
        data: stores,
        generator: ( store ) => {
            console.log( `Processing ${store.label}` );
            return getStoreMapByNumber( store.storeNumber ).then( results => {
                if ( 0 === results.length ) {
                    return addNewStore( store );
                } else {
                    console.log( store.label + ' already exists.' );
                    return results[0];
                }
            } );
        }
    } ).promise().then( ( results ) => {
        console.log( results.length + ' stores processed.' );
    } );
} );