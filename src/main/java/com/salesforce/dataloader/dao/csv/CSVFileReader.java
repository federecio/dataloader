/*
 * Copyright (c) 2012, salesforce.com, inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 *    the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package com.salesforce.dataloader.dao.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.salesforce.dataloader.config.Config;
import com.salesforce.dataloader.config.Messages;
import com.salesforce.dataloader.controller.Controller;
import com.salesforce.dataloader.dao.DataReader;
import com.salesforce.dataloader.exception.DataAccessObjectException;
import com.salesforce.dataloader.exception.DataAccessObjectInitializationException;
import com.salesforce.dataloader.exception.DataAccessRowException;
import com.salesforce.dataloader.model.Row;
import com.salesforce.dataloader.util.DAORowUtil;
import com.sforce.async.CSVReader;

/**
 * Wrapper around {@link CSVReader} that allows to read CSV files
 *
 * @author Federico Recio
 */
public class CSVFileReader implements DataReader {

    private static Logger logger = Logger.getLogger(CSVFileReader.class);
    private File file;
    private BufferedReader input;
    // this should store the total rows in the file,
    // must be set externally, since this class streams the file
    private int totalRows;
    private CSVReader csvReader;
    private int currentRowNumber;
    private boolean forceUTF8;
    private List<String> headerRow;
    private boolean isOpen;

    public CSVFileReader(Config config) {
        this(new File(config.getString(Config.DAO_NAME)), config);
    }

    public CSVFileReader(String filePath, Controller controller) {
        this(new File(filePath), controller.getConfig());
    }

    public CSVFileReader(File file, Config config) {
        this.file = file;
        if (config.isBulkAPIEnabled()) {
            forceUTF8 = true;
        } else {
            forceUTF8 = config.getBoolean(Config.READ_UTF8);
        }
    }

    /**
     * Should the file always be read as UTF8
     *
     * @return true if UTF8 format is forced
     */

    protected boolean isForceUTF8() {
        return forceUTF8;
    }

    /**
     * Sets if the file should always be read as UTF8
     *
     * @param utf8Encoded
     */
    public void setForceUTF8(boolean utf8Encoded) {
        this.forceUTF8 = utf8Encoded;
    }

    @Override
    public void checkConnection() throws DataAccessObjectInitializationException {
        open();
        close();
    }

    @Override
    public void open() throws DataAccessObjectInitializationException {
        if (isOpen()) {
            close();
        }

        try {
            if (forceUTF8 || isUTF8File(file)) {
                csvReader = new CSVReader(new FileInputStream(file), "UTF-8");
            } else {
                csvReader = new CSVReader(new FileInputStream(file));
            }
        } catch (FileNotFoundException e) {
            String errMsg = Messages.getFormattedString("CSVFileDAO.errorOpen", file.getAbsolutePath()); //$NON-NLS-1$
            logger.error(errMsg, e);
            throw new DataAccessObjectInitializationException(errMsg, e);
        } catch (UnsupportedEncodingException e) {
            logger.error(Messages.getString("CSVFileDAO.errorUnsupportedEncoding"), e); //$NON-NLS-1$
            throw new DataAccessObjectInitializationException(Messages.getString("CSVFileDAO.errorUnsupportedEncoding"), e); //$NON-NLS-1$
        }

        try {
            headerRow = csvReader.nextRecord();

            if (headerRow == null) {
                logger.error(Messages.getString("CSVFileDAO.errorHeaderRow")); //$NON-NLS-1$
                throw new DataAccessObjectInitializationException(Messages.getString("CSVFileDAO.errorHeaderRow")); //$NON-NLS-1$
            }

            // file is open and initialized at this point
            setOpen(true);
        } catch (IOException e) {
            logger.error(Messages.getString("CSVFileDAO.errorHeaderRow")); //$NON-NLS-1$
            throw new DataAccessObjectInitializationException(Messages.getString("CSVFileDAO.errorHeaderRow")); //$NON-NLS-1$
        } finally {
            // if there's a problem getting header row, the stream needs to be closed
            if (!isOpen()) {
                try {
                    input.close();
                } catch (IOException ignore) {
                    // ignore exception as this is ok at this point
                }
            }
        }
    }

    /**
     * Close the file stream when we are finishe with a load
     */
    @Override
    public void close() {
        try {
            if (isOpen() && input != null) {
                try {
                    input.close();
                } catch (IOException ioe) {
                    logger.error("Error closing file stream.", ioe);
                }
            }
        } finally {
            input = null;
            csvReader = null;
            setOpen(false);
        }
    }

    /**
     * Checks the Bytes for the UTF-8 BOM if found, returns true, else false
     */
    private boolean isUTF8File(File f) {

        FileInputStream stream = null;

        // UTF-8 BOM is 0xEE 0xBB OxBf
        // or 239 187 191

        try {
            stream = new FileInputStream(f);

            if (stream.read() == 239) {
                if (stream.read() == 187) {
                    if (stream.read() == 191) {
                        return true;
                    }
                }
            }
        } catch (FileNotFoundException e) {
            logger.error("Error in file when testing CSV");
        } catch (IOException io) {
            logger.error("IO error when testing file");
        } finally {
            IOUtils.closeQuietly(stream);
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * @see com.salesforce.dataloader.dao.DataReader#readRowList(int)
     */
    @Override
    public List<Row> readRowList(int maxRows) throws DataAccessObjectException {
        List<Row> outputRows = new ArrayList<Row>();
        for (int i = 0; i < maxRows; i++) {
            Row outputRow = readRow();
            if (outputRow != null) {
                // if row has been returned, add it to the output
                outputRows.add(outputRow);
            } else {
                // if encountered null, the reading is over
                break;
            }
        }
        return outputRows;
    }

    /*
     * Gets the next row from the current data access object data source. <i>Side effect:</i> Updates the current record
     * number
     * @see com.salesforce.dataloader.dao.DataAccessObject#getNextRow()
     */
    @Override
    public synchronized Row readRow() throws DataAccessObjectException {
        // make sure file is open
        if (!isOpen()) {
            open();
        }

        List<String> record;
        try {
            record = csvReader.nextRecord();
        } catch (IOException e) {
            throw new DataAccessObjectException(e);
        }

        if (!DAORowUtil.isValidRow(record)) {
            return null;       // EOF would be better to use some kind of Null Pattern.
        }

        if (record.size() != headerRow.size()) {
            String errMsg = Messages
                    .getFormattedString(
                            "CSVFileDAO.errorRowTooLarge", new String[] { //$NON-NLS-1$
                            String.valueOf(currentRowNumber), String.valueOf(record.size()),
                            String.valueOf(headerRow.size()) });
            throw new DataAccessRowException(errMsg);
        }

        Row row = new Row(record.size());

        for(int i = 0; i < headerRow.size(); i++) {
            String value = record.get(i);
            if(value == null) {
                value = "";
            }
            row.put(headerRow.get(i), value);
        }
        currentRowNumber++;
        return row;
    }

    /**
     * @return Names of output columns being read during each readRow call
     */
    @Override
    public List<String> getColumnNames() {
        return headerRow;
    }

    /*
     * (non-Javadoc)
     * @see com.salesforce.dataloader.dao.DataReader#getTotalRows()
     */
    @Override
    public int getTotalRows() throws DataAccessObjectException {
        if (totalRows == 0) {
            assert isOpen();
            totalRows = DAORowUtil.calculateTotalRows(this);
        }
        return totalRows;
    }

    /**
     * @return Current record number that has been read
     */
    @Override
    public int getCurrentRowNumber() {
        return currentRowNumber;
    }

    public boolean isOpen() {
        return isOpen;
    }

    public void setOpen(boolean isOpen) {
        this.isOpen = isOpen;
    }
}
